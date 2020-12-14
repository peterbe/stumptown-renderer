import React, { Suspense, lazy, useEffect, useState } from "react";

import { AUTOCOMPLETE_SEARCH_WIDGET } from "../../../constants";
import { useLocale } from "../../../hooks";

import "./index.scss";

// The temporary solution... See below around the BasicSearchWidget component.
import "./basic-search-widget.scss";
// import { SearchNavigateWidget } from "../../../search";
const LazySearchNavigateWidget = lazy(() => import("./lazy-search-widget"));

export function Search(props) {
  const [
    useAutocompleteSearchWidget,
    setUseAutocompleteSearchWidget,
  ] = useState(false);
  useEffect(() => {
    if (AUTOCOMPLETE_SEARCH_WIDGET) {
      setUseAutocompleteSearchWidget(true);
    }
  }, []);

  return (
    <div className="header-search">
      {/* See the code comment next to the <BasicSearchWidget> component */}
      {useAutocompleteSearchWidget ? (
        <Suspense fallback={<BasicSearchWidget />}>
          <LazySearchNavigateWidget {...props} />
        </Suspense>
      ) : (
        <BasicSearchWidget />
      )}
    </div>
  );
}

// This is a TEMPORARY solution.
// In the Yari1 launch we want to disable the auto-complete search that Yari has.
// Instead we want it to look and behave just like the search widget you get
// on the Kuma pages.
// The general idea is that we keep it like this for the period of time when
// the home page and site-search pages are still rendered in Kuma.
// Once all pages come from Yari, we'll get rid of this.
// For more context and discussion see https://github.com/mdn/yari/issues/1663

export function BasicSearchWidget() {
  const locale = useLocale();
  return (
    <form action={`/${locale}/search`} className="search-form">
      <label htmlFor="main-q" className="visually-hidden">
        Search MDN
      </label>
      <input
        type="search"
        name="q"
        id="main-q"
        className="search-input-field"
        placeholder="Search MDN"
        pattern="(.|\s)*\S(.|\s)*"
        required
      />
      <input
        type="submit"
        className="ghost search-button"
        value=""
        aria-label="Search"
      />
    </form>
  );
}
