import React, { lazy, Suspense, useEffect, useState } from "react";
import useSWR from "swr";

import { DisplayH2, DisplayH3 } from "./ingredients/utils";

// Because it's bad for web performance to lazy-load CSS during the initial render
// (because the page is saying "Wait! Stop rendering, now that I've downloaded
// some JS I decided I need more CSSOM to block the rendering.")
// Therefore, we import all the necessary CSS here in this file so that
// the BCD table CSS becomes part of the core bundle.
// That means that when the lazy-loading happens, it only needs to lazy-load
// the JS (and the JSON XHR fetch of course)
import "./ingredients/browser-compatibility-table/index.scss";

const BrowserCompatibilityTable = lazy(
  () => import("./ingredients/browser-compatibility-table")
);

const isServer = typeof window === "undefined";

export function LazyBrowserCompatibilityTable({
  id,
  title,
  isH3,
  query,
  dataURL,
}: {
  id: string;
  title: string;
  isH3: boolean;
  query: string;
  dataURL: string;
}) {
  return (
    <>
      {title && !isH3 && <DisplayH2 id={id} title={title} />}
      {title && isH3 && <DisplayH3 id={id} title={title} />}
      <LazyBrowserCompatibilityTableInner dataURL={dataURL} query={query} />
    </>
  );
}

function LazyBrowserCompatibilityTableInner({
  dataURL,
  query,
}: {
  dataURL: string;
  query: string;
}) {
  const [bcdDataURL, setBCDDataURL] = useState("");

  const { error, data } = useSWR(
    bcdDataURL ? bcdDataURL : null,
    async (url) => {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(await response.text());
      }
      return await response.json();
    },
    { revalidateOnFocus: false }
  );

  useEffect(() => {
    setBCDDataURL(dataURL);
  }, [dataURL]);

  if (isServer) {
    return <p>BCD tables only load in the browser</p>;
  }
  if (!data && !error) {
    return <Loading />;
  }
  if (error) {
    return <p>Error loading BCD data</p>;
  }
  if (!data.data) {
    return (
      <p>
        No compatibility data found for <code>{query}</code>. Check the spelling
        or contribute data to{" "}
        <a href="https://github.com/mdn/browser-compat-data">
          mdn/browser-compat-data
        </a>
        .
      </p>
    );
  }

  return (
    <Suspense fallback={<Loading />}>
      <BrowserCompatibilityTable {...data} />
    </Suspense>
  );
}

function Loading() {
  return <p>Loading BCD table...</p>;
}
