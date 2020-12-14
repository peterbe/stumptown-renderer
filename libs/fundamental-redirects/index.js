const { VALID_LOCALES, LOCALE_ALIASES } = require("../constants");

const startRe = /^\^?\/?/;
const startTemplate = /^\//;

function redirect(pattern, template, options = {}) {
  return (path) => {
    const match = pattern.exec(path);
    if (match === null) {
      return null;
    }
    const status = options.permanent ? 301 : 302;
    if (typeof template === "string") {
      return { url: template, status };
    }
    const { [0]: subString, index, groups } = match;
    const before = path.substring(0, index);
    const after = path.substring(index + subString.length);
    const to = template({ ...groups });
    return { url: `${before}${to}${after}`, status };
  };
}

function localeRedirect(
  pattern,
  template,
  { prependLocale = true, ...options } = {}
) {
  const patternStrWithLocale = pattern.source.replace(
    startRe,
    "^(?<locale>\\w{2,3}(?:-\\w{2})?/)?"
  );
  const patternWithLocale = new RegExp(patternStrWithLocale, pattern.flags);
  let _template = template;
  if (prependLocale) {
    _template = ({ locale, ...group } = {}) =>
      `/${locale ? `${locale}` : ""}${(typeof template === "string"
        ? template
        : template(group)
      ).replace(startTemplate, "")}`;
  }
  return redirect(patternWithLocale, _template, options);
}

function externalRedirect(pattern, template, options = {}) {
  return localeRedirect(pattern, template, {
    prependLocale: false,
    permanent: true,
    ...options,
  });
}

const fixableLocales = new Map();
for (const locale of VALID_LOCALES.keys()) {
  if (locale.includes("-")) {
    // E.g. `en-US` becomes alias `en_US`
    fixableLocales.set(locale.replace("-", "_").toLowerCase(), locale);
  } else {
    // E.g. `fr` becomes alias `fr-XX`
    fixableLocales.set(`${locale}-\\w{2}`.toLowerCase(), locale);
  }
}

for (const [alias, correct] of LOCALE_ALIASES) {
  // E.g. things like `en` -> `en-us` or `pt` -> `pt-pt`
  fixableLocales.set(alias, correct);
}

// All things like `/en_Us/docs/...` -> `/en-US/docs/...`
const LOCALE_PATTERNS = [
  redirect(
    new RegExp(
      `^(?<locale>${Array.from(fixableLocales.keys()).join(
        "|"
      )})(/(?<suffix>.*)|$)`,
      "i"
    ),
    ({ locale, suffix }) => {
      locale = locale.toLowerCase();
      if (fixableLocales.has(locale)) {
        // E.g. it was something like `en_Us`
        locale = VALID_LOCALES.get(fixableLocales.get(locale).toLowerCase());
      } else {
        // E.g. it was something like `Fr-sW` (Swiss French)
        locale = locale.split("-")[0];
        locale = VALID_LOCALES.get(locale);
      }
      return `/${locale}/${suffix || ""}`;
    },
    { permanent: true }
  ),
];

// Redirects/rewrites/aliases migrated from SCL3 httpd config
const SCL3_REDIRECT_PATTERNS = [
  // RewriteRule ^/media/(redesign/)?css/(.*)-min.css$
  // /static/build/styles/$2.css [L,R=301]
  redirect(
    /^media\/(?:redesign\/)?css\/(?<doc>.*)-min.css$/i,
    ({ doc }) => `/static/build/styles/${doc}.css`,
    { permanent: true }
  ),
  // RewriteRule ^/media/(redesign/)?js/(.*)-min.js$ /static/build/js/$2.js
  // [L,R=301]
  redirect(
    /^media\/(?:redesign\/)?js\/(?<doc>.*)-min.js$/i,
    ({ doc }) => `/static/build/js/${doc}.js`,
    { permanent: true }
  ),
  // RewriteRule ^/media/(redesign/)?img(.*) /static/img$2 [L,R=301]
  redirect(
    /^media\/(?:redesign\/)?img(?<suffix>.*)$/i,
    ({ suffix }) => `/static/img${suffix}`,
    { permanent: true }
  ),
  // RewriteRule ^/media/(redesign/)?css(.*) /static/styles$2 [L,R=301]
  redirect(
    /^media\/(?:redesign\/)?css(?<suffix>.*)$/i,
    ({ suffix }) => `/static/styles${suffix}`,
    { permanent: true }
  ),
  // RewriteRule ^/media/(redesign/)?js(.*) /static/js$2 [L,R=301]
  redirect(
    /^media\/(?:redesign\/)?js(?<suffix>.*)$/i,
    ({ suffix }) => `/static/js${suffix}`,
    { permanent: true }
  ),
  // RewriteRule ^/media/(redesign/)?fonts(.*) /static/fonts$2 [L,R=301]
  redirect(
    /^media\/(?:redesign\/)?fonts(?<suffix>.*)$/i,
    ({ suffix }) => `/static/fonts${suffix}`,
    { permanent: true }
  ),
  // RedirectMatch 302 /media/uploads/demos/(.*)$
  // https://developer.mozilla.org/docs/Web/Demos_of_open_web_technologies/
  // Django will then redirect based on Accept-Language
  redirect(
    /^media\/uploads\/demos\/(?:.*)$/i,
    "/docs/Web/Demos_of_open_web_technologies/",
    { permanent: false }
  ),
  // RewriteRule ^(.*)//(.*)//(.*)$ $1_$2_$3 [R=301,L,NC]
  redirect(
    /^(?<one>.*)\/\/(?<two>.*)\/\/(?<three>.*)$/i,
    ({ one, two, three }) => `/${one}_${two}_${three}`,
    { permanent: true }
  ),
  // RewriteRule ^(.*)//(.*)$ $1_$2 [R=301,L,NC]
  redirect(/^(?<one>.*)\/\/(?<two>.*)$/i, ({ one, two }) => `/${one}_${two}`, {
    permanent: true,
  }),
  // The remaining redirects don't show explicit RewriteRule as comments,
  // as they're all in the style of "static URL A now points at static URL B"
  // Bug 1078186 - Redirect old static canvas examples to wiki pages
  // canvas tutorial
  redirect(
    /^samples\/canvas-tutorial\/2_1_canvas_rect.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Drawing_shapes#Rectangular_shape_example",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/2_2_canvas_moveto.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Drawing_shapes#Moving_the_pen",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/2_3_canvas_lineto.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Drawing_shapes#Lines",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/2_4_canvas_arc.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Drawing_shapes#Arcs",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/2_5_canvas_quadraticcurveto.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Drawing_shapes#Quadratic_Bezier_curves",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/2_6_canvas_beziercurveto.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Drawing_shapes#Cubic_Bezier_curves",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/3_1_canvas_drawimage.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Using_images#Drawing_images",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/3_2_canvas_drawimage.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Using_images#Example.3A_Tiling_an_image",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/3_3_canvas_drawimage.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Using_images#Example.3A_Framing_an_image",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/3_4_canvas_gallery.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Using_images#Art_gallery_example",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_1_canvas_fillstyle.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.fillStyle",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_2_canvas_strokestyle.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.strokeStyle",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_3_canvas_globalalpha.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.globalAlpha",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_4_canvas_rgba.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Applying_styles_and_colors#An_example_using_rgba()",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_5_canvas_linewidth.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Applying_styles_and_colors#A_lineWidth_example",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_6_canvas_linecap.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.lineCap",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_7_canvas_linejoin.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.lineJoin",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_8_canvas_miterlimit.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.miterLimit",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_9_canvas_lineargradient.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Applying_styles_and_colors#A_createLinearGradient_example",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_10_canvas_radialgradient.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Applying_styles_and_colors#A_createRadialGradient_example",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/4_11_canvas_createpattern.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.createPattern",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/5_1_canvas_savestate.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Transformations#A_save_and_restore_canvas_state_example",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/5_2_canvas_translate.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.translate",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/5_3_canvas_rotate.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.rotate",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/5_4_canvas_scale.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.scale",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/6_1_canvas_composite.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.globalCompositeOperation",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/6_2_canvas_clipping.html$/i,
    "/docs/Web/API/Canvas_API/Tutorial/Compositing#Clipping_paths",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/globalCompositeOperation.html$/i,
    "/docs/Web/API/CanvasRenderingContext2D.globalCompositeOperation",
    { permanent: true }
  ),
  //##################################
  // MOZILLADEMOS
  //##################################
  // canvas images
  redirect(
    /^samples\/canvas-tutorial\/images\/backdrop.png$/i,
    "https://mdn.mozillademos.org/files/5395/backdrop.png",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/bg_gallery.png$/i,
    "https://mdn.mozillademos.org/files/5415/bg_gallery.png",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/gallery_1.jpg$/i,
    "https://mdn.mozillademos.org/files/5399/gallery_1.jpg",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/gallery_2.jpg$/i,
    "https://mdn.mozillademos.org/files/5401/gallery_2.jpg",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/gallery_3.jpg$/i,
    "https://mdn.mozillademos.org/files/5403/gallery_3.jpg",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/gallery_4.jpg$/i,
    "https://mdn.mozillademos.org/files/5405/gallery_4.jpg",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/gallery_5.jpg$/i,
    "https://mdn.mozillademos.org/files/5407/gallery_5.jpg",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/gallery_6.jpg$/i,
    "https://mdn.mozillademos.org/files/5409/gallery_6.jpg",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/gallery_7.jpg$/i,
    "https://mdn.mozillademos.org/files/5411/gallery_7.jpg",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/gallery_8.jpg$/i,
    "https://mdn.mozillademos.org/files/5413/gallery_8.jpg",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/picture_frame.png$/i,
    "https://mdn.mozillademos.org/files/242/Canvas_picture_frame.png",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/rhino.jpg$/i,
    "https://mdn.mozillademos.org/files/5397/rhino.jpg",
    { permanent: true }
  ),
  redirect(
    /^samples\/canvas-tutorial\/images\/wallpaper.png$/i,
    "https://mdn.mozillademos.org/files/222/Canvas_createpattern.png",
    { permanent: true }
  ),
  // canvas example in samples/domref
  redirect(
    /^samples\/domref\/mozGetAsFile.html$/i,
    "/docs/Web/API/HTMLCanvasElement.mozGetAsFile",
    { permanent: true }
  ),
  //##################################
  // MDN.GITHUB.IO
  //##################################
  // canvas raycaster
  redirect(
    /^samples\/raycaster\/input.js$/i,
    "http://mdn.github.io/canvas-raycaster/input.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/raycaster\/Level.js$/i,
    "http://mdn.github.io/canvas-raycaster/Level.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/raycaster\/Player.js$/i,
    "http://mdn.github.io/canvas-raycaster/Player.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/raycaster\/RayCaster.html$/i,
    "http://mdn.github.io/canvas-raycaster/index.html",
    { permanent: true }
  ),
  redirect(
    /^samples\/raycaster\/RayCaster.js$/i,
    "http://mdn.github.io/canvas-raycaster/RayCaster.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/raycaster\/trace.css$/i,
    "http://mdn.github.io/canvas-raycaster/trace.css",
    { permanent: true }
  ),
  redirect(
    /^samples\/raycaster\/trace.js$/i,
    "http://mdn.github.io/canvas-raycaster/trace.js",
    { permanent: true }
  ),
  // Bug 1215255 - Redirect static WebGL examples
  redirect(
    /^samples\/webgl\/sample1$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample1",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample1\/index.html$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample1/index.html",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample1\/webgl-demo.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample1/webgl-demo.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample1\/webgl.css$/i,
    "http://mdn.github.io/webgl-examples/tutorial/webgl.css",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample2$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample2",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample2\/glUtils.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/glUtils.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample2\/index.html$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample2/index.html",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample2\/sylvester.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sylvester.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample2\/webgl-demo.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample2/webgl-demo.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample2\/webgl.css$/i,
    "http://mdn.github.io/webgl-examples/tutorial/webgl.css",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample3$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample3",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample3\/glUtils.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/glUtils.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample3\/index.html$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample3/index.html",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample3\/sylvester.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sylvester.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample3\/webgl-demo.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample3/webgl-demo.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample3\/webgl.css$/i,
    "http://mdn.github.io/webgl-examples/tutorial/webgl.css",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample4$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample4",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample4\/glUtils.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/glUtils.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample4\/index.html$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample4/index.html",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample4\/sylvester.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sylvester.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample4\/webgl-demo.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample4/webgl-demo.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample4\/webgl.css$/i,
    "http://mdn.github.io/webgl-examples/tutorial/webgl.css",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample5$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample5",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample5\/glUtils.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/glUtils.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample5\/index.html$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample5/index.html",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample5\/sylvester.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sylvester.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample5\/webgl-demo.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample5/webgl-demo.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample5\/webgl.css$/i,
    "http://mdn.github.io/webgl-examples/tutorial/webgl.css",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample6$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample6",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample6\/cubetexture.png$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample6/cubetexture.png",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample6\/glUtils.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/glUtils.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample6\/index.html$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample6/index.html",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample6\/sylvester.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sylvester.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample6\/webgl-demo.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample6/webgl-demo.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample6\/webgl.css$/i,
    "http://mdn.github.io/webgl-examples/tutorial/webgl.css",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample7$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample7",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample7\/cubetexture.png$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample7/cubetexture.png",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample7\/glUtils.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/glUtils.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample7\/index.html$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample7/index.html",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample7\/sylvester.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sylvester.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample7\/webgl-demo.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample7/webgl-demo.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample7\/webgl.css$/i,
    "http://mdn.github.io/webgl-examples/tutorial/webgl.css",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample8$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample8",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample8\/Firefox.ogv$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample8/Firefox.ogv",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample8\/glUtils.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/glUtils.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample8\/index.html$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample8/index.html",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample8\/sylvester.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sylvester.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample8\/webgl-demo.js$/i,
    "http://mdn.github.io/webgl-examples/tutorial/sample8/webgl-demo.js",
    { permanent: true }
  ),
  redirect(
    /^samples\/webgl\/sample8\/webgl.css$/i,
    "http://mdn.github.io/webgl-examples/tutorial/webgl.css",
    { permanent: true }
  ),
  // All of the remaining "samples/" URL's are redirected to the
  // the media domain (ATTACHMENTS_AWS_S3_CUSTOM_DOMAIN).
  redirect(
    /^samples\/(?<sample_path>.*)$/i,
    ({ sample_path }) =>
      `https://media.prod.mdn.mozit.cloud/samples/${sample_path}`,
    { permanent: false }
  ),
  // Bug 887428 - Misprinted URL in promo materials
  // RewriteRule ^Firefox_OS/Security$ docs/Mozilla/Firefox_OS/Security
  // [R=301,L,NC]
  redirect(/^Firefox_OS\/Security$/i, "/docs/Mozilla/Firefox_OS/Security", {
    permanent: true,
  }),
  // Old landing pages. The regex, adapted from Bedrock, captures locale prefixes.
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?mobile/?$ /$1docs/Mozilla/Mobile
  // [R=301,L]
  localeRedirect(/^mobile\/?$/i, "/docs/Mozilla/Mobile", { permanent: true }),
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?addons/?$ /$1Add-ons [R=301,L]
  localeRedirect(/^addons\/?$/i, "/Add-ons", { permanent: true }),
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?mozilla/?$ /$1docs/Mozilla [R=301,L]
  localeRedirect(/^mozilla\/?$/i, "/docs/Mozilla", { permanent: true }),
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?web/?$ /$1docs/Web [R=301,L]
  localeRedirect(/^web\/?$/i, "/docs/Web", { permanent: true }),
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?learn/html5/?$
  // /$1docs/Web/Guide/HTML/HTML5 [R=301,L]
  localeRedirect(/^learn\/html5\/?$/i, "/docs/Web/Guide/HTML/HTML5", {
    permanent: true,
  }),
  // Some blanket section moves / renames
  // RewriteRule ^En/JavaScript/Reference/Objects/Array$
  // en-US/docs/JavaScript/Reference/Global_Objects/Array [R=301,L,NC]
  redirect(
    /^En\/JavaScript\/Reference\/Objects\/Array$/i,
    "/en-US/docs/JavaScript/Reference/Global_Objects/Array",
    { permanent: true }
  ),
  // RewriteRule ^En/JavaScript/Reference/Objects$
  // en-US/docs/JavaScript/Reference/Global_Objects/Object [R=301,L,NC]
  redirect(
    /^En\/JavaScript\/Reference\/Objects$/i,
    "/en-US/docs/JavaScript/Reference/Global_Objects/Object",
    { permanent: true }
  ),
  // RewriteRule ^En/Core_JavaScript_1\.5_Reference/Objects/(.*)
  // en-US/docs/JavaScript/Reference/Global_Objects/$1 [R=301,L,NC]
  redirect(
    /^En\/Core_JavaScript_1\.5_Reference\/Objects\/(?<suffix>.*)$/i,
    ({ suffix }) => `/en-US/docs/JavaScript/Reference/Global_Objects/${suffix}`,
    { permanent: true }
  ),
  // RewriteRule ^En/Core_JavaScript_1\.5_Reference/(.*)
  // en-US/docs/JavaScript/Reference/$1 [R=301,L,NC]
  redirect(
    /^En\/Core_JavaScript_1\.5_Reference\/(?<suffix>.*)$/i,
    ({ suffix }) => `/en-US/docs/JavaScript/Reference/${suffix}`,
    { permanent: true }
  ),
  // RewriteRule ^([\w\-]*)/HTML5$ $1/docs/HTML/HTML5 [R=301,L,NC]
  localeRedirect(/^HTML5$/i, "/docs/HTML/HTML5", { permanent: true }),
  // RewriteRule web-tech/2008/09/12/css-transforms
  // /docs/CSS/Using_CSS_transforms [R=301,L]
  redirect(
    /^web-tech\/2008\/09\/12\/css-transforms$/i,
    "/docs/CSS/Using_CSS_transforms",
    { permanent: true }
  ),
  // RewriteRule ^([\w\-]*)/docs/?$ $1/docs/Web [R=301,L,NC]
  localeRedirect(/^docs\/?$/i, "/docs/Web", { permanent: true }),
  // DevNews
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?devnews/index.php/feed.*
  // https://blog.mozilla.org/feed/ [R=301,L]
  localeRedirect(
    /^devnews\/index.php\/feed.*/i,
    "https://blog.mozilla.org/feed/",
    { prependLocale: false, permanent: true }
  ),
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?devnews.*
  // https://wiki.mozilla.org/Releases [R=301,L]
  localeRedirect(/^devnews.*/i, "https://wiki.mozilla.org/Releases", {
    prependLocale: false,
    permanent: true,
  }),
  // Old "Learn" pages
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?learn/html /$1Learn/HTML [R=301,L]
  localeRedirect(/^learn\/html/i, "/docs/Learn/HTML", { permanent: true }),
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?learn/css /$1Learn/CSS [R=301,L]
  localeRedirect(/^learn\/css/i, "/docs/Learn/CSS", { permanent: true }),
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?learn/javascript /$1Learn/JavaScript
  // [R=301,L]
  localeRedirect(/^learn\/javascript/i, "/docs/Learn/JavaScript", {
    permanent: true,
  }),
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?learn /$1Learn [R=301,L]
  localeRedirect(/^learn/i, "/docs/Learn", { permanent: true }),
  // BananaBread demo (bug 1238041)
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?demos/detail/bananabread$
  // https://github.com/kripken/BananaBread/ [R=301,L]
  localeRedirect(
    /^demos\/detail\/bananabread$/i,
    "https://github.com/kripken/BananaBread/",
    { prependLocale: false, permanent: true }
  ),
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?demos/detail/bananabread/launch$
  // https://kripken.github.io/BananaBread/cube2/index.html [R=301,L]
  localeRedirect(
    /^demos\/detail\/bananabread\/launch$/i,
    "https://kripken.github.io/BananaBread/cube2/index.html",
    { prependLocale: false, permanent: true }
  ),
  // All other Demo Studio and Dev Derby paths (bug 1238037)
  // RewriteRule ^(\w{2,3}(?:-\w{2})?/)?demos
  // /$1docs/Web/Demos_of_open_web_technologies? [R=301,L]
  localeRedirect(/^demos.*/i, "/docs/Web/Demos_of_open_web_technologies", {
    permanent: true,
  }),
  // Legacy off-site redirects (bug 1362438)
  // RewriteRule ^contests/ http://www.mozillalabs.com/ [R=302,L]
  redirect(/^contests.*/i, "http://www.mozillalabs.com/", { permanent: false }),
  // RewriteRule ^es4 http://www.ecma-international.org/memento/TC39.htm [R=302,L]
  redirect(/^es4.*/i, "http://www.ecma-international.org/memento/TC39.htm", {
    permanent: false,
  }),
];

const zoneRedirects = [
  [
    "Add-ons",
    "Mozilla/Add-ons",
    [
      "ar",
      "bn",
      "ca",
      "de",
      "en-US",
      "es",
      "fa",
      "fr",
      "hu",
      "id",
      "it",
      "ja",
      "ms",
      "nl",
      "pl",
      "pt-BR",
      "pt-PT",
      "ru",
      "sv-SE",
      "th",
      "uk",
      "vi",
      "zh-CN",
      "zh-TW",
      null,
    ],
  ],
  ["Add-ons", "Mozilla/Πρόσθετα", ["el"]],
  ["Add-ons", "Mozilla/애드온들", ["ko"]],
  ["Add-ons", "Mozilla/Eklentiler", ["tr"]],
  [
    "Firefox",
    "Mozilla/Firefox",
    [
      "ar",
      "bm",
      "ca",
      "de",
      "el",
      "en-US",
      "es",
      "fi",
      "fr",
      "he",
      "hi-IN",
      "hu",
      "id",
      "it",
      "ja",
      "ko",
      "ms",
      "my",
      "nl",
      "pl",
      "pt-BR",
      "pt-PT",
      "ru",
      "sv-SE",
      "th",
      "tr",
      "vi",
      "zh-CN",
      "zh-TW",
      null,
    ],
  ],
  ["Firefox", "Mozilla/ফায়ারফক্স", ["bn"]],
  [
    "Apps",
    "Web/Apps",
    ["en-US", "fa", "fr", "ja", "th", "zh-CN", "zh-TW", null],
  ],
  ["Apps", "Web/Aplicaciones", ["es"]],
  ["Apps", "Apps", ["bn", "de", "it", "ko", "pt-BR", "ru"]],
  ["Learn", "Learn", ["ca", "de", null]],
  ["Apprendre", "Apprendre", ["fr"]],
  [
    "Marketplace",
    "Mozilla/Marketplace",
    ["de", "en-US", "es", "fr", "it", "ja", "zh-CN", null],
  ],
  ["Marketplace", "Mozilla/بازار", ["fa"]],
];

const zonePatternFmt = (prefix, zoneRootPattern) =>
  new RegExp(`^${prefix}${zoneRootPattern}(?:\\/?|(?<subPath>[\\/$].+))$`, "i");
const subPathFmt = (prefix, wikiSlug) => ({ subPath = "" } = {}) =>
  `/${prefix}docs/${wikiSlug}${subPath}`;

const ZONE_REDIRECT_PATTERNS = [];
for (const [zoneRoot, wikiSlug, locales] of zoneRedirects) {
  for (const locale of locales) {
    let zoneRootPattern = zoneRoot;
    if (zoneRoot !== wikiSlug) {
      zoneRootPattern = "(?:docs/)?" + zoneRootPattern;
    }
    // NOTE: The redirect for the case when there is no locale for a zone
    // must be handled here, because if we let LocaleMiddleware handle the
    // 404 response and redirect to the proper locale, the path would be
    // considered invalid.
    const prefix = locale ? locale + "/" : "";
    const pattern = zonePatternFmt(prefix, zoneRootPattern);
    const subPath = subPathFmt(prefix, wikiSlug);
    ZONE_REDIRECT_PATTERNS.push(
      redirect(
        pattern,
        subPath,
        { permanent: false }
        // TODO: (decorators = shared_cache_control_for_zones)
      )
    );
  }
}

const MARIONETTE_CLIENT_DOCS_URL =
  "https://marionette-client.readthedocs.io/en/latest/";
const MARIONETTE_DOCS_ROOT_URL =
  "https://firefox-source-docs.mozilla.org/testing/marionette/marionette/";

const MARIONETTE_REDIRECT_PATTERNS = [
  externalRedirect(
    /docs\/(?:Mozilla\/QA\/)?Marionette$/i,
    () => `${MARIONETTE_DOCS_ROOT_URL}index.html`
  ),
  externalRedirect(
    /docs\/(?:Mozilla\/QA\/)?Marionette\/Builds$/i,
    () => `${MARIONETTE_DOCS_ROOT_URL}Building.html`
  ),
  externalRedirect(
    /docs\/(?:Mozilla\/QA\/)?Marionette\/Client$/i,
    () => MARIONETTE_CLIENT_DOCS_URL
  ),
  externalRedirect(
    /docs\/Mozilla\/QA\/Marionette\/Python_Client$/i,
    () => MARIONETTE_CLIENT_DOCS_URL
  ),
  externalRedirect(
    /docs\/(?:Mozilla\/QA\/)?Marionette\/Developer_setup$/i,
    () => `${MARIONETTE_DOCS_ROOT_URL}Contributing.html`
  ),
  externalRedirect(
    /docs\/Marionette_Test_Runner$/i,
    () => `${MARIONETTE_DOCS_ROOT_URL}PythonTests.html`
  ),
  externalRedirect(
    /docs\/Mozilla\/QA\/Marionette\/Marionette_Test_Runner$/i,
    () => `${MARIONETTE_DOCS_ROOT_URL}PythonTests.html`
  ),
  externalRedirect(
    /docs\/(?:Mozilla\/QA\/)?Marionette\/(?:MarionetteTestCase|Marionette_Python_Tests|Running_Tests|Tests)$/i,
    () => `${MARIONETTE_DOCS_ROOT_URL}PythonTests.html`
  ),
  externalRedirect(
    /docs\/Mozilla\/QA\/Marionette\/Protocol$/i,
    () => `${MARIONETTE_DOCS_ROOT_URL}Protocol.html`
  ),
  externalRedirect(
    /docs\/Mozilla\/QA\/Marionette\/WebDriver\/status$/i,
    () =>
      "https://bugzilla.mozilla.org/showdependencytree.cgi?id=721859&hide_resolved=1"
  ),
  externalRedirect(
    /docs\/Marionette\/Debugging$/i,
    () => `${MARIONETTE_DOCS_ROOT_URL}Debugging.html`
  ),
];

const WEBEXTENSIONS_REDIRECT_PATTERNS = [];
for (const [aoPath, ewPath] of [
  [
    "WebExtensions/Security_best_practices",
    "develop/build-a-secure-extension/",
  ],
  [
    "WebExtensions/user_interface/Accessibility_guidelines",
    "develop/build-an-accessible-extension/",
  ],
  [
    "WebExtensions/onboarding_upboarding_offboarding_best_practices",
    "develop/onboard-upboard-offboard-users/",
  ],
  [
    "WebExtensions/Porting_a_Google_Chrome_extension",
    "develop/porting-a-google-chrome-extension/",
  ],
  [
    "WebExtensions/Porting_a_legacy_Firefox_add-on",
    "develop/porting-a-legacy-firefox-extension/",
  ],
  [
    "WebExtensions/Comparison_with_the_Add-on_SDK",
    "develop/comparison-with-the-add-on-sdk/",
  ],
  [
    "WebExtensions/Comparison_with_XUL_XPCOM_extensions",
    "develop/comparison-with-xul-xpcom-extensions/",
  ],
  [
    "WebExtensions/Differences_between_desktop_and_Android",
    "develop/differences-between-desktop-and-android-extensions/",
  ],
  [
    "WebExtensions/Development_Tools",
    "develop/browser-extension-development-tools/",
  ],
  [
    "WebExtensions/Choose_a_Firefox_version_for_web_extension_develop",
    "develop/choosing-a-firefox-version-for-extension-development/",
  ],
  [
    "WebExtensions/User_experience_best_practices",
    "develop/user-experience-best-practices/",
  ],
  [
    "WebExtensions/Prompt_users_for_data_and_privacy_consents",
    "develop/best-practices-for-collecting-user-data-consents/",
  ],
  [
    "WebExtensions/Temporary_Installation_in_Firefox",
    "develop/temporary-installation-in-firefox/",
  ],
  ["WebExtensions/Debugging", "develop/debugging/"],
  [
    "WebExtensions/Testing_persistent_and_restart_features",
    "develop/testing-persistent-and-restart-features/",
  ],
  [
    "WebExtensions/Test_permission_requests",
    "develop/test-permission-requests/",
  ],
  [
    "WebExtensions/Developing_WebExtensions_for_Firefox_for_Android",
    "develop/developing-extensions-for-firefox-for-android/",
  ],
  [
    "WebExtensions/Getting_started_with_web-ext",
    "develop/getting-started-with-web-ext/",
  ],
  [
    "WebExtensions/web-ext_command_reference",
    "develop/web-ext-command-reference/",
  ],
  [
    "WebExtensions/WebExtensions_and_the_Add-on_ID",
    "develop/extensions-and-the-add-on-id/",
  ],
  [
    "WebExtensions/Request_the_right_permissions",
    "develop/request-the-right-permissions/",
  ],
  [
    "WebExtensions/Best_practices_for_updating_your_extension",
    "manage/best-practices-for-updating/",
  ],
  ["Updates", "manage/updating-your-extension/"],
  [
    "WebExtensions/Distribution_options",
    "publish/signing-and-distribution-overview/",
  ],
  [
    "Themes/Using_the_AMO_theme_generator",
    "themes/using-the-amo-theme-generator/",
  ],
  ["WebExtensions/Developer_accounts", "publish/developer-accounts/"],
  [
    "Distribution",
    "publish/signing-and-distribution-overview/#distributing-your-addon",
  ],
  ["WebExtensions/Package_your_extension_", "publish/package-your-extension/"],
  ["Distribution/Submitting_an_add-on", "publish/submitting-an-add-on/"],
  ["Source_Code_Submission", "publish/source-code-submission/"],
  ["Distribution/Resources_for_publishers", "manage/resources-for-publishers/"],
  ["Listing", "develop/create-an-appealing-listing/"],
  [
    "Distribution/Make_money_from_browser_extensions",
    "publish/make-money-from-browser-extensions/",
  ],
  [
    "Distribution/Promoting_your_extension_or_theme",
    "publish/promoting-your-extension/",
  ],
  ["AMO/Policy/Reviews", "publish/add-on-policies/"],
  ["AMO/Policy/Agreement", "publish/firefox-add-on-distribution-agreement/"],
  ["Distribution/Retiring_your_extension", "manage/retiring-your-extension/"],
  [
    "WebExtensions/Distribution_options/Sideloading_add-ons",
    "publish/distribute-sideloading/",
  ],
  [
    "WebExtensions/Distribution_options/Add-ons_for_desktop_apps",
    "publish/distribute-for-desktop-apps/",
  ],
  [
    "WebExtensions/Distribution_options/Add-ons_in_the_enterprise",
    "enterprise/",
  ],
  ["AMO/Blocking_Process", "publish/add-ons-blocking-process/"],
  ["Third_Party_Library_Usage", "publish/third-party-library-usage/"],
  [
    "WebExtensions/What_does_review_rejection_mean_to_users",
    "publish/what-does-review-rejection-mean-to-users/",
  ],
  ["AMO/Policy/Featured", "publish/recommended-extensions/"],
]) {
  WEBEXTENSIONS_REDIRECT_PATTERNS.push(
    externalRedirect(
      new RegExp(`docs\\/Mozilla\\/Add-ons\\/${aoPath}$`, "i"),
      `https://extensionworkshop.com/documentation/${ewPath}`
    )
  );
}

const FIREFOX_ACCOUNTS_REDIRECT_PATTERNS = [
  externalRedirect(
    /(?:docs\/)?Mozilla\/Firefox_Accounts_OAuth_Dashboard(?:\/|$)/i,
    "https://mozilla.github.io/ecosystem-platform/docs/welcome"
  ),
  externalRedirect(
    /(?:docs\/)?Mozilla\/(?:Tech\/)?Firefox_Accounts(?:\/|$)/i,
    "https://mozilla.github.io/ecosystem-platform/docs/welcome"
  ),
  externalRedirect(
    /(?:docs\/)?Archive\/Mozilla\/Firefox\/Accounts(?:\/|$)/i,
    "https://mozilla.github.io/ecosystem-platform/docs/welcome"
  ),
];

const FIREFOX_SOURCE_DOCS_ROOT_URL = "https://firefox-source-docs.mozilla.org";
const FIREFOX_SOURCE_DOCS_REDIRECT_PATTERNS = [];
for (const [pattern, path] of [
  [
    /(?:docs\/)?Mozilla\/Memory_Sanitizer(?:\/|$)/i,
    "/tools/sanitizer/memory_sanitizer.html#memory-sanitizer",
  ],
  [
    /(?:docs\/)?Debugging_Mozilla_with_gdb(?:\/|$)/i,
    "/contributing/debugging/debugging_firefox_with_gdb.html",
  ],
  [
    /(?:docs\/)?Debugging_Mozilla_with_lldb(?:\/|$)/i,
    "/contributing/debugging/debugging_firefox_with_lldb.html",
  ],
  [
    /(?:docs\/)?Understanding_crash_reports(?:\/|$)/i,
    "/contributing/debugging/understanding_crash_reports.html",
  ],
  [
    /(?:docs\/)?Debugging_a_minidump(?:\/|$)/i,
    "/contributing/debugging/debugging_a_minidump.html",
  ],
  [
    /(?:docs\/)?Debugging_Mozilla_with_Valgrind(?:\/|$)/i,
    "/contributing/debugging/debugging_firefox_with_valgrind.html",
  ],
  [
    /(?:docs\/)?Debugging\/Record_and_Replay_Debugging_Firefox(?:\/|$)/i,
    "/contributing/debugging/debugging_firefox_with_rr.html",
  ],
]) {
  FIREFOX_SOURCE_DOCS_REDIRECT_PATTERNS.push(
    externalRedirect(pattern, FIREFOX_SOURCE_DOCS_ROOT_URL + path)
  );
}

const REDIRECT_PATTERNS = [].concat(
  SCL3_REDIRECT_PATTERNS,
  ZONE_REDIRECT_PATTERNS,
  MARIONETTE_REDIRECT_PATTERNS,
  WEBEXTENSIONS_REDIRECT_PATTERNS,
  FIREFOX_ACCOUNTS_REDIRECT_PATTERNS,
  FIREFOX_SOURCE_DOCS_REDIRECT_PATTERNS,
  [
    localeRedirect(
      /^fellowship.*/i,
      "/docs/Archive/2015_MDN_Fellowship_Program",
      {
        permanent: true,
      }
    ),
    localeRedirect(
      /^docs\/(ServerJS|CommonJS)(?<subPath>$|\/.+)/i,
      ({ subPath }) => `https://wiki.mozilla.org/docs/ServerJS${subPath}`,
      { prependLocale: false, permanent: true }
    ),
  ],
  LOCALE_PATTERNS
);

const STARTING_SLASH = /^\//;
const ABSOLUTE_URL = /^https?:\/\/.*/;

function resolveFundamental(path) {
  if (ABSOLUTE_URL.exec(path)) {
    return {};
  }
  const trimmedPath = path.replace(STARTING_SLASH, "");
  for (const resolve of REDIRECT_PATTERNS) {
    const redirect = resolve(trimmedPath);
    if (redirect) {
      return redirect;
    }
  }
  return {};
}

module.exports = {
  resolveFundamental,
};
