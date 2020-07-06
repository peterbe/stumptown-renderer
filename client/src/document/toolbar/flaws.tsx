import React, { useEffect, useReducer, useRef } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { annotate, annotationGroup } from "rough-notation";
import { RoughAnnotation } from "rough-notation/lib/model";

import { humanizeFlawName } from "../../flaw-utils";
import { Doc, Link, MacroErrorMessage } from "../types";
import "./flaws.scss";

interface FlawCount {
  name: string;
  count: number;
}

const FLAWS_HASH = "#_flaws";
export function ToggleDocumentFlaws({ doc }: { doc: Doc }) {
  const location = useLocation();
  const navigate = useNavigate();
  const [show, toggle] = useReducer((v) => !v, location.hash === FLAWS_HASH);
  const rootElement = useRef<HTMLDivElement>(null);
  const isInitialRender = useRef(true);

  useEffect(() => {
    if (isInitialRender.current && show && rootElement.current) {
      rootElement.current.scrollIntoView({ behavior: "smooth" });
    }
    isInitialRender.current = false;
  }, [show]);

  useEffect(() => {
    const hasShowHash = window.location.hash === FLAWS_HASH;
    if (show && !hasShowHash) {
      navigate(location.pathname + location.search + FLAWS_HASH);
    } else if (!show && hasShowHash) {
      navigate(location.pathname + location.search);
    }
  }, [location, navigate, show]);

  const flawsCounts = Object.entries(doc.flaws)
    .map(([name, actualFlaws]) => ({
      name,
      count: actualFlaws.length,
    }))
    .sort((a, b) => b.count - a.count);

  return (
    <div id={FLAWS_HASH.slice(1)} ref={rootElement}>
      {flawsCounts.length > 0 ? (
        <button type="submit" onClick={toggle}>
          {show
            ? "Hide flaws"
            : `Show flaws (${flawsCounts
                .map((flaw) => flaw.count)
                .join(" + ")})`}
        </button>
      ) : (
        <p>
          No known flaws at the moment
          <span role="img" aria-label="yay!">
            🍾
          </span>
        </p>
      )}

      {show ? (
        <Flaws doc={doc} flaws={flawsCounts} />
      ) : (
        <small>
          {/* a one-liner about all the flaws */}
          {flawsCounts
            .map((flaw) => `${humanizeFlawName(flaw.name)}: ${flaw.count}`)
            .join(" + ")}
        </small>
      )}
    </div>
  );
}

function Flaws({ doc, flaws }: { doc: Doc; flaws: FlawCount[] }) {
  if (process.env.NODE_ENV !== "development") {
    throw new Error("This shouldn't be used in non-development builds");
  }
  return (
    <div id="document-flaws">
      {flaws.map((flaw) => {
        switch (flaw.name) {
          case "broken_links":
            return (
              <BrokenLinks
                key="broken_links"
                sourceFolder={doc.source.folder}
                links={doc.flaws.broken_links}
              />
            );
          case "bad_bcd_queries":
            return (
              <BadBCDQueries
                key="bad_bcd_queries"
                messages={doc.flaws.bad_bcd_queries}
              />
            );
          case "macros":
            return <Macros key="macros" messages={doc.flaws.macros} />;
          default:
            throw new Error(`Unknown flaw check '${flaw.name}'`);
        }
      })}
    </div>
  );
}

function BrokenLinks({
  sourceFolder,
  links,
}: {
  sourceFolder: string;
  links: Link[];
}) {
  const [opening, setOpening] = React.useState<string | null>(null);
  useEffect(() => {
    let unsetOpeningTimer: ReturnType<typeof setTimeout>;
    if (opening) {
      unsetOpeningTimer = setTimeout(() => {
        setOpening(null);
      }, 3000);
    }
    return () => {
      if (unsetOpeningTimer) {
        clearTimeout(unsetOpeningTimer);
      }
    };
  }, [opening]);

  const filepath = sourceFolder + "/index.html";

  function equalUrls(url1: string, url2: string) {
    return (
      new URL(url1, "http://example.com").pathname ===
      new URL(url2, "http://example.com").pathname
    );
  }

  function openInEditor(key: string, line: number, column: number) {
    const sp = new URLSearchParams();
    sp.set("filepath", filepath);
    sp.set("line", `${line}`);
    sp.set("column", `${column}`);
    console.log(
      `Going to try to open ${filepath}:${line}:${column} in your editor`
    );
    setOpening(key);
    fetch(`/_open?${sp.toString()}`).catch((err) => {
      console.warn(`Error trying to _open?${sp.toString()}:`, err);
    });
  }

  useEffect(() => {
    const annotations: RoughAnnotation[] = [];

    let linkIndex = 0;
    for (const anchor of [
      ...document.querySelectorAll<HTMLAnchorElement>("div.content a[href]"),
    ]) {
      const link = links[linkIndex];
      if (!link) {
        break;
      }
      // A `anchor.href` is always absolute with `http//localhost/...`.
      // But `link.href` is not necessarily so, but it might.
      // The only "hashing" that matters is the pathname.
      if (!equalUrls(anchor.href, link.href)) {
        continue;
      }
      linkIndex++;
      const annotationColor = link.suggestion ? "orange" : "red";
      anchor.dataset.originalTitle = anchor.title;
      anchor.title = link.suggestion
        ? `Consider fixing! Suggestion: ${link.suggestion}`
        : "Broken link! Links to a page that will not be found";
      annotations.push(
        annotate(anchor, {
          type: "box",
          color: annotationColor,
          animationDuration: 300,
        })
      );
    }

    const ag = annotationGroup(annotations);
    ag.show();

    return () => {
      ag.hide();

      // Now, restore any 'title' attributes that were overridden.
      for (const anchor of Array.from(
        document.querySelectorAll<HTMLAnchorElement>(`div.content a`)
      )) {
        if (anchor.dataset.originalTitle !== undefined) {
          anchor.title = anchor.dataset.originalTitle;
        }
      }
    };
  }, [links]);

  return (
    <div className="flaw flaw__broken_links">
      <h3>Broken Links</h3>
      <ol>
        {links.map((link, i) => {
          const key = `${link.href}${link.line}${link.column}`;
          return (
            <li key={key}>
              <code>{link.href}</code>{" "}
              {link.suggestion && (
                <span>
                  Suggested fix: <code>{link.suggestion}</code>
                </span>
              )}{" "}
              <span
                role="img"
                aria-label="Click to highlight broken link"
                title="Click to highlight broken link anchor"
                style={{ cursor: "zoom-in" }}
                onClick={() => {
                  const annotations: RoughAnnotation[] = [];

                  let linkIndex = 0;
                  for (const anchor of [
                    ...document.querySelectorAll<HTMLAnchorElement>(
                      "div.content a[href]"
                    ),
                  ]) {
                    const link = links[linkIndex];
                    if (!link) {
                      break;
                    }
                    if (!equalUrls(anchor.href, link.href)) {
                      continue;
                    }

                    if (i === linkIndex) {
                      anchor.scrollIntoView({
                        behavior: "smooth",
                        block: "center",
                      });

                      if (anchor.parentElement) {
                        annotations.push(
                          annotate(anchor, {
                            type: "circle",
                            color: "purple",
                            animationDuration: 500,
                            strokeWidth: 2,
                            padding: 6,
                          })
                        );
                      }
                    }
                    linkIndex++;
                  }

                  if (annotations.length) {
                    const ag = annotationGroup(annotations);
                    ag.show();
                    // Only show this extra highlight temporarily
                    window.setTimeout(() => {
                      ag.hide();
                    }, 2000);
                  }
                }}
              >
                👀
              </span>{" "}
              <a
                href={`file://${filepath}`}
                onClick={(event: React.MouseEvent) => {
                  event.preventDefault();
                  openInEditor(key, link.line, link.column);
                }}
                title="Click to open in your editor"
              >
                line {link.line}:{link.column}
              </a>{" "}
              {opening && opening === key && <small>Opening...</small>}
            </li>
          );
        })}
      </ol>
    </div>
  );
}

function BadBCDQueries({ messages }) {
  return (
    <div className="flaw flaw__bad_bcd_queries">
      <h3>{humanizeFlawName("bad_bcd_queries")}</h3>
      <ul>
        {messages.map((message) => (
          <li key={message}>
            <code>{message}</code>
          </li>
        ))}
      </ul>
    </div>
  );
}

function Macros({ messages }: { messages: MacroErrorMessage[] }) {
  const [opening, setOpening] = React.useState<string | null>(null);
  useEffect(() => {
    let unsetOpeningTimer: ReturnType<typeof setTimeout>;
    if (opening) {
      unsetOpeningTimer = setTimeout(() => {
        setOpening(null);
      }, 3000);
    }
    return () => {
      if (unsetOpeningTimer) {
        clearTimeout(unsetOpeningTimer);
      }
    };
  }, [opening]);

  function openInEditor(msg: MacroErrorMessage, key: string) {
    const sp = new URLSearchParams();
    sp.set("filepath", msg.filepath);
    sp.set("line", `${msg.line}`);
    sp.set("column", `${msg.column}`);
    console.log(
      `Going to try to open ${msg.filepath}:${msg.line}:${msg.column} in your editor`
    );
    setOpening(key);
    fetch(`/_open?${sp.toString()}`);
  }

  useEffect(() => {
    const typesById = new Map(
      messages
        .filter((msg) => msg.id)
        .map((msg) => {
          // Turn it into a string because, as as dataset attribute it's
          // actually always a string
          return [`${msg.id}`, msg];
        })
    );
    console.log(typesById);

    const annotations: RoughAnnotation[] = [];
    for (const anchor of [
      ...document.querySelectorAll<HTMLAnchorElement>(
        "div.content a[data-flaw-id]"
      ),
    ]) {
      if (anchor.dataset.flawId && typesById.has(anchor.dataset.flawId)) {
        const msg: MacroErrorMessage = typesById.get(anchor.dataset.flawId);
        const annotationColor =
          msg.name === "MacroRedirectedLinkError" ? "pink" : "red";
        // anchor.dataset.originalTitle = anchor.title;
        annotations.push(
          annotate(anchor, {
            type: "box",
            color: annotationColor,
            animationDuration: 300,
          })
        );
      }
    }

    const ag = annotationGroup(annotations);
    ag.show();

    return () => {
      ag.hide();
    };
  }, [messages]);
  return (
    <div className="flaw flaw__macros">
      <h3>{humanizeFlawName("macros")}</h3>
      {messages.map((msg) => {
        const key = `${msg.filepath}:${msg.line}:${msg.column}`;

        return (
          <details key={key}>
            <summary>
              <a
                href={`file://${msg.filepath}`}
                onClick={(event: React.MouseEvent) => {
                  event.preventDefault();
                  openInEditor(msg, key);
                }}
              >
                <code>{msg.name}</code> from <code>{msg.macroName}</code> in
                line {msg.line}:{msg.column}
              </a>{" "}
              {opening && opening === key && <small>Opening...</small>}
            </summary>
            <b>Context:</b>
            <pre>{msg.sourceContext}</pre>
            <b>Original error message:</b>
            <pre>{msg.errorMessage}</pre>
            <b>Filepath:</b>
            <br />
            <code>{msg.filepath}</code>
          </details>
        );
      })}
    </div>
  );
}
