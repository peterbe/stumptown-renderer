import React, { useEffect, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";

import "./edit-actions.scss";

export function EditActions({ folder }: { folder: string }) {
  const location = useLocation();

  const [opening, setOpening] = useState(false);
  const [editorOpeningError, setEditorOpeningError] = useState<Error | null>(
    null
  );

  useEffect(() => {
    let unsetOpeningTimer: ReturnType<typeof setTimeout>;
    if (opening) {
      unsetOpeningTimer = setTimeout(() => {
        setOpening(false);
      }, 3000);
    }
    return () => {
      if (unsetOpeningTimer) {
        clearTimeout(unsetOpeningTimer);
      }
    };
  }, [opening]);

  async function openInEditorHandler(event: React.MouseEvent) {
    event.preventDefault();

    const filepath = folder + "/index.html";
    console.log(`Going to try to open ${filepath} in your editor`);
    setOpening(true);
    try {
      const response = await fetch(`/_open?filepath=${filepath}`);
      if (!response.ok) {
        if (response.status >= 500) {
          setEditorOpeningError(
            new Error(`${response.status}: ${response.statusText}`)
          );
        } else {
          const body = await response.text();
          setEditorOpeningError(new Error(`${response.status}: ${body}`));
        }
      }
    } catch (err) {
      setEditorOpeningError(err);
    }
  }

  const { locale, "*": slug } = useParams();

  if (!folder) {
    return null;
  }

  return (
    <ul className="edit-actions">
      <li>
        <button
          type="button"
          className="button"
          title={`Folder: ${folder}`}
          onClick={openInEditorHandler}
        >
          Open in your <b>editor</b>
        </button>
      </li>

      <li>
        <a
          href={`https://developer.mozilla.org/${locale}/docs/${slug}`}
          className="button"
        >
          View on MDN
        </a>
      </li>

      <li>
        <Link
          to={location.pathname.replace("/docs/", "/_edit/")}
          className="button"
        >
          Quick-edit
        </Link>
      </li>

      {editorOpeningError ? (
        <p className="error-message editor-opening-error">
          <b>Error opening page in your editor!</b>
          <br />
          <code>{editorOpeningError.toString()}</code>
        </p>
      ) : (
        opening && <small>Trying to your editor now...</small>
      )}
    </ul>
  );
}
