import React, { useReducer } from "react";
import { useLocation } from "react-router-dom";
import type bcd from "@mdn/browser-compat-data/types";
import { BrowserInfoContext } from "./browser-info";
import { BrowserCompatibilityErrorBoundary } from "./error-boundary";
import { FeatureRow } from "./feature-row";
import { PLATFORM_BROWSERS, Headers } from "./headers";
import { Legend } from "./legend";
import { listFeatures } from "./utils";

// Note! Don't import any SCSS here inside *this* component.
// It's done in the component that lazy-loads this component.

// This string is used to prefill the body when clicking to file a new BCD
// issue over on github.com/mdn/browser-compat-data
const NEW_ISSUE_TEMPLATE = `
<!-- Tips: where applicable, specify browser name, browser version, and mobile operating system version -->

#### What information was incorrect, unhelpful, or incomplete?

#### What did you expect to see?

#### Did you test this? If so, how?


<!-- Do not make changes below this line -->
<details>
<summary>MDN page report details</summary>

* Query: \`$QUERY_ID\`
* MDN URL: https://developer.mozilla.org$PATHNAME
* Report started: $DATE

</details>
`;

function gatherPlatformsAndBrowsers(
  category: string
): [string[], bcd.BrowserNames[]] {
  let platforms = ["desktop", "mobile"];
  if (category === "javascript") {
    platforms.push("server");
  } else if (category === "webextensions") {
    platforms = ["webextensions-desktop", "webextensions-mobile"];
  }
  return [
    platforms,
    platforms.map((platform) => PLATFORM_BROWSERS[platform] || []).flat(),
  ];
}

type CellIndex = [number, number];

function FeatureListAccordion({
  features,
  browsers,
}: {
  features: ReturnType<typeof listFeatures>;
  browsers: bcd.BrowserNames[];
}) {
  const [[activeRow, activeColumn], dispatchCellToggle] = useReducer<
    React.Reducer<CellIndex | [null, null], CellIndex>
  >(
    ([activeRow, activeColumn], [row, column]) =>
      activeRow === row && activeColumn === column
        ? [null, null]
        : [row, column],
    [null, null]
  );

  return (
    <>
      {features.map((feature, i) => (
        <FeatureRow
          key={i}
          {...{ feature, browsers }}
          index={i}
          activeCell={activeRow === i ? activeColumn : null}
          onToggleCell={dispatchCellToggle}
        />
      ))}
    </>
  );
}

export default function BrowserCompatibilityTable({
  query,
  data,
  browsers: browserInfo,
}: {
  query: string;
  data: bcd.Identifier;
  browsers: bcd.Browsers;
}) {
  const location = useLocation();

  if (!data || !Object.keys(data).length) {
    throw new Error(
      "BrowserCompatibilityTable component called with empty data"
    );
  }

  const breadcrumbs = query.split(".");
  const category = breadcrumbs[0];
  const name = breadcrumbs[breadcrumbs.length - 1];

  const [platforms, browsers] = gatherPlatformsAndBrowsers(category);

  function getNewIssueURL() {
    const url = "https://github.com/mdn/browser-compat-data/issues/new";
    const sp = new URLSearchParams();
    const body = NEW_ISSUE_TEMPLATE.replace(/\$PATHNAME/g, location.pathname)
      .replace(/\$DATE/g, new Date().toISOString())
      .replace(/\$QUERY_ID/g, query)
      .trim();
    sp.set("body", body);
    sp.set("title", `${query} - <PUT TITLE HERE>`);
    return `${url}?${sp.toString()}`;
  }

  return (
    <BrowserCompatibilityErrorBoundary>
      <BrowserInfoContext.Provider value={browserInfo}>
        <a
          className="bc-github-link external external-icon"
          href={getNewIssueURL()}
          target="_blank"
          rel="noopener noreferrer"
          title="Report an issue with this compatibility data"
        >
          Report problems with this compatibility data on GitHub
        </a>
        <table key="bc-table" className="bc-table bc-table-web">
          <Headers {...{ platforms, browsers }} />
          <tbody>
            <FeatureListAccordion
              browsers={browsers}
              features={listFeatures(data, "", name)}
            />
          </tbody>
        </table>
        <Legend compat={data} />

        {/* https://github.com/mdn/yari/issues/1191 */}
        <div className="hidden">
          The compatibility table on this page is generated from structured
          data. If you'd like to contribute to the data, please check out{" "}
          <a href="https://github.com/mdn/browser-compat-data">
            https://github.com/mdn/browser-compat-data
          </a>{" "}
          and send us a pull request.
        </div>
      </BrowserInfoContext.Provider>
    </BrowserCompatibilityErrorBoundary>
  );
}
