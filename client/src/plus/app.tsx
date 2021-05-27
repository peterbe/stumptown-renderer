import React from "react";

import "./fonts/metropolis.css";
import "./fonts/inter.css";
import "./index.scss";

export default function App() {
  const [showDeepDive, setShowDeepDive] = React.useState(false);

  return (
    <div className="plus">
      <main>
        <header>
          <div className="header-wrapper">
            <div className="header-content">
              <h2>MDN Plus</h2>
              <h1>
                More MDN. <span>Your MDN.</span>
              </h1>
              <p>
                <b>Coming soon</b> — a new premium service with monthly
                technical deep dives written by industry experts and powerful
                new features to personalize your MDN experience.
              </p>
            </div>
            <div className="header-illustration">
              <div className="mandala" />
            </div>
          </div>
        </header>
        <section>
          <div className="feature-wrapper">
            <div className="section-feature-2-col">
              <div>
                <h2>What is MDN Plus</h2>
                <p>
                  MDN Plus builds on top of your much-loved core content,
                  providing constantly-updated guides to highly-requested
                  topics, helping you keep your knowledge fresh and your skills
                  sharp. In addition, MDN Plus includes tools to make MDN more
                  powerful for you, creating a more personalized experience.
                </p>
              </div>

              <div>
                <h2>What does this mean for MDN Web Docs</h2>
                <p>
                  <b>Nothing</b> is changing with the existing MDN Web Docs
                  content — this content will continue to be free and available
                  to everyone. We want to provide extra value through premium
                  content and features to help make MDN self-sustaining, on a
                  completely opt-in basis. Again,<b> nothing is changing </b>{" "}
                  with the existing MDN Web Docs!
                </p>
              </div>
            </div>
          </div>
        </section>
        <section className="purple-bg main-feature">
          <div className="feature-wrapper main-feature-wrapper">
            <figure className="deepdive-mobile" />
            <h2>What's included</h2>
            <div className="section-feature-2-col">
              <div className="section-main-content">
                <h1>
                  Boost your learning <span>with MDN Deep Dives</span>
                </h1>
                <p>
                  Receive access to new deep dives every month, written by
                  Mozilla and industry insiders. Upcoming titles include:
                </p>
                <ul>
                  <li>Modern CSS in the real world</li>
                  <li>A robust CSS pattern library</li>
                  <li>Modern responsive web design</li>
                  <li>Security considerations in web development</li>
                  <li>
                    GDPR, DSAR, CCPA, and COPPA. So many acronyms! Learn
                    Mozilla's framework to handle privacy laws
                  </li>
                </ul>
              </div>
              <div className="section-main-illustration">
                <figure className="deepdive" />
              </div>
            </div>
          </div>
        </section>
        <section>
          <div className="feature-wrapper">
            <h3 style={{ paddingBottom: 50 }}>
              Read an excerpt from{" "}
              <span className="magenta-bg">
                "Your browser support toolkit",
              </span>{" "}
              the second article from the forthcoming deep dive{" "}
              <i>Modern CSS in the real world</i> by Rachel Andrew.
            </h3>
            <div className="section-feature-1-2-col">
              <div className="deep-dive-card">
                <h4>Featured Deep Dive</h4>
                <h2>
                  Modern CSS in the real world: Your browser support toolkit
                </h2>
                By Rachel Andrew
                <hr />
                <ul>
                  <li>This three part series includes:</li>
                  <li>Planning for browser support</li>
                  <li>
                    {" "}
                    <span className="magenta-bg">
                      Your browser support toolkit
                    </span>
                  </li>
                  <li>Practical browser support</li>
                </ul>
              </div>
              <div>
                <div className={showDeepDive ? "" : "fade"}>
                  <h4>Web platform features and fallbacks</h4>
                  <p>
                    If you have discovered that a feature isn’t supported in a
                    browser, but still intend to use it, you might need to
                    create a fallback for browsers that don't support it. In
                    addition, if you are allowing browsers without support to
                    fall back to a basic layout, you need to make sure that the
                    code aimed at modern browsers doesn’t leak through to older
                    browsers and make a mess.
                  </p>
                  <p>
                    In recent years creating CSS fallbacks has become much
                    easier and CSS has native features that can help you. Two of
                    the most powerful are the cascade and feature queries, and
                    we'll explore these now. Later on we'll also look at how
                    vendor prefixes can be a useful tool as long as they are
                    used carefully.
                  </p>
                  <h4>Using the cascade </h4>
                  <p>
                    The first thing to look at is how the cascade works with
                    properties and values that are not understood by a browser.
                    We can create simple fallbacks by writing CSS for old
                    browsers, then following it with CSS aimed at newer
                    browsers. For example, you might want to provide a simple
                    solid background color for really old browsers, and a
                    semi-transparent color for newer browsers:
                  </p>
                  <div className="code-snippet">
                    <code>
                      <span className="code-c">background-color</span>: red;
                      <br />
                      <span className="code-c">background-color</span>: rgba(
                      <span className="code-m">255</span>,
                      <span className="code-m">0</span>,
                      <span className="code-m">0</span>,
                      <span className="code-m">0.6</span>);
                    </code>
                  </div>
                </div>
                {showDeepDive && (
                  <div>
                    <p>
                      The idea is that older browsers support the first
                      declaration and so will apply it to the page, then treat
                      the second one as invalid because they don't support it —
                      this means they completely ignore it. Newer browsers will
                      support both declarations, however the rules of the
                      cascade mean that the declaration that comes later in the
                      stylesheet will override the earlier one, and be used by
                      the browser.
                    </p>

                    <p>
                      CSS also has rules defining what happens when there are
                      two potentially conflicting things being applied to an
                      element. For example, if you have a floated item and its
                      parent becomes a grid container, the floated item stops
                      behaving like a floated item and becomes a grid item. We
                      can see how this works in the following demo.
                    </p>
                    <p>
                      In this example, the component has a simple, floated
                      layout. This is the layout that browsers without CSS Grid
                      support will use. For newer browsers the container has
                      been turned into a grid container, which means that in a
                      browser with CSS grid support the float is not applied.
                    </p>
                    <div className="code-snippet">
                      <div className="codepen">
                        <iframe
                          id="cp_embed_MWJVaqm"
                          src="https://codepen.io/rachelandrew/embed/qBrZVVm?height=450&amp;theme-id=1&amp;slug-hash=qBrZVVm&amp;default-tab=css,result"
                          scrolling="no"
                          allowFullScreen={false}
                          title="Modern CSS 2:2"
                          className="codepen"
                          loading="lazy"
                          style={{
                            width: "100%",
                            overflow: "hidden",
                            height: "100%",
                          }}
                          frameBorder={0}
                        ></iframe>
                      </div>
                    </div>
                    <h4>Feature Queries</h4>
                    <p>
                      For very simple fallbacks, the overriding method shown
                      previously may work. It can however require that you order
                      the declarations and rules in your CSS carefully, making
                      it more brittle than you might like. You may also run into
                      problems when you want to use additional CSS to enhance
                      the layout in newer browsers, if that CSS is also
                      understood by older browsers.
                    </p>
                    <p>
                      In the next demo, I have given the left-hand column a
                      background color. I only want this to apply to the CSS
                      Grid layout, where I can ensure that the columns will be
                      the same height as each other. However, using the previous
                      method the background color is understood and therefore
                      used by browsers without CSS Grid support too. I have also
                      added widths to the floated elements. As a percentage
                      width is interpreted by the grid layout as a percentage of
                      the column track, this causes the columns to become
                      narrower than the track.
                    </p>
                    <div className="code-snippet">
                      <div className="codepen">
                        <iframe
                          id="cp_embed_MWJVaqm"
                          src="https://codepen.io/rachelandrew/embed/gOmrXqQ?height=450&amp;theme-id=1&amp;slug-hash=gOmrXqQ&amp;default-tab=css,result"
                          scrolling="no"
                          allowFullScreen={false}
                          title="Modern CSS 2.3 before"
                          className="codepen"
                          loading="lazy"
                          style={{
                            width: "100%",
                            overflow: "hidden",
                            height: "100%",
                          }}
                          frameBorder={0}
                        ></iframe>
                      </div>
                    </div>
                    <p>
                      In situations like this, CSS Feature Queries are useful. A
                      feature query is similar to a media query, however instead
                      of testing to see how large the viewport is, we are
                      testing to see if a browser has support for that feature.
                    </p>
                    <p>
                      Introducing a Feature Query into our demo means that we
                      can wrap up all of our grid code with a test to see if the
                      browser supports <code>display: grid</code>.
                    </p>
                    <div className="code-snippet">
                      <div className="codepen">
                        <iframe
                          id="cp_embed_MWJVaqm"
                          src="https://codepen.io/rachelandrew/embed/xxqVPjq?height=450&amp;theme-id=1&amp;slug-hash=MWJVaqm&amp;default-tab=css,result"
                          scrolling="no"
                          allowFullScreen={false}
                          title="Modern CSS 2.3 after"
                          className="codepen"
                          loading="lazy"
                          style={{
                            width: "100%",
                            overflow: "hidden",
                            height: "100%",
                          }}
                          frameBorder={0}
                        ></iframe>
                      </div>
                    </div>
                    <p>
                      Feature Queries are a simple test to see if the browser
                      can parse a given declaration. They can’t tell you if the
                      browser supports that feature without bugs, however with a
                      good knowledge of browser support they can be a great way
                      to safely add enhancements to a design. In the next
                      article we will look at some more realistic components and
                      how to use this method to build in progressively enhanced
                      support.
                    </p>
                    <div className="deep-dive-sidebar">
                      <p>
                        You can also test for feature support using JavaScript,
                        using{" "}
                        <a href="(https://developer.mozilla.org/en-US/docs/Web/API/CSS/supports">
                          CSS.supports()
                        </a>
                        . As with Feature Queries in CSS, this function takes a
                        property and value as arguments. Therefore, to test for
                        CSS Grid layout support, you would use:
                      </p>
                      <div className="code-snippet">
                        <code>
                          <span className="code-c">let</span>{" "}
                          <span className="code-m">result</span> ={" "}
                          <span className="code-c">CSS</span>.
                          <span className="code-y">supports</span>(“display”,
                          “grid”);
                        </code>
                      </div>
                      <p>
                        The returned result is true or false, indicating if the
                        browser does or does not have support.
                      </p>
                    </div>
                  </div>
                )}
                {!showDeepDive && (
                  <button
                    type="button"
                    onClick={() => {
                      setShowDeepDive(!showDeepDive);
                    }}
                    aria-expanded={showDeepDive}
                  >
                    Expand deep dive
                  </button>
                )}
              </div>
            </div>
            {showDeepDive && (
              <h3>
                Liked what you read? Want more? Sign up for the waitlist and be
                the first notified when we launch!{" "}
                <a href="#waitlist"> To waitlist » </a>
              </h3>
            )}
          </div>
        </section>
        <section className="purple-bg secondary-feature">
          <div className="feature-wrapper secondary-feature-wrapper">
            <figure className="premiumfeatures-mobile" />
            <div className="section-feature-1-2-col">
              <div className="section-secondary-illustration">
                <figure className="premiumfeatures" />
              </div>
              <div className="section-secondary-content">
                <h2>What's included</h2>
                <h1>Make MDN your own</h1>
                <p style={{ fontSize: 20 }}>
                  Unlock premium features that you can use across all of MDN.
                </p>
              </div>
            </div>
          </div>
        </section>
        <section className="white-bg">
          <div className="feature-wrapper">
            <div className="section-feature-3-col">
              <div className="tile">
                <figure className="bookmark" />
                <h1>Build a permanent library</h1>
                Bookmark and annotate free and paid content for reference across
                devices.
              </div>
              <div className="tile">
                <figure className="offline" />
                <h1>Take MDN with you</h1>
                Download MDN documentation and deep dives for access offline.
              </div>
              <div className="tile">
                <figure className="bcdtable" />
                <h1>Customize compat tables</h1>
                Display customised data sets based on the browsers your projects
                need to support.
              </div>
            </div>
          </div>
        </section>
      </main>
    </div>
  );
}
