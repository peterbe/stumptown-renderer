import { Redirect } from "@reach/router";
import FlexSearch from "flexsearch";
import React from "react";
// import fuzzy from "fuzzy.js";
// import FuzzySearch from "fuzzy-search";
import FuzzySearch from "./fuzzy-search";
import "./search.scss";

// XXX replace this with a matchMedia instead.
// The only reason this is used is to make differences on a smaller
// screen. For example the number of search results shouldn't
// depend on the navigator but instead of the screen real estate.
function isMobileUserAgent() {
  return (
    typeof window !== "undefined" &&
    (typeof window.orientation !== "undefined" ||
      navigator.userAgent.indexOf("IEMobile") !== -1)
  );
}

export class SearchWidget extends React.Component {
  state = {
    highlitResult: null,
    initializing: false,
    lastQ: "",
    q: "",
    redirectTo: null,
    searchResults: [],
    serverError: null,
    showSearchResults: true
  };

  ACTIVE_PLACEHOLDER = "Go ahead. Type your search...";
  INACTIVE_PLACEHOLDER = isMobileUserAgent()
    ? "Site search..."
    : 'Site search... (Press "/" to focus)';

  inFocus = false;

  focusOnSearchMaybe = event => {
    if (event.code === "Slash") {
      if (!this.inFocus) {
        event.preventDefault();
        this.inputRef.current.focus();
      }
    }
  };

  getCurrentLocale = () => {
    return (
      (this.props.pathname && this.props.pathname.split("/")[1]) || "en-US"
    );
  };

  componentDidMount() {
    document.addEventListener("keydown", this.focusOnSearchMaybe);
  }

  componentDidUpdate(prevProps) {
    if (prevProps.pathname !== this.props.pathname) {
      // Hide search results if you changed page.
      if (this.state.showSearchResults || this.state.q) {
        this.setState({
          highlitResult: null,
          lastQ: "",
          q: "",
          redirectTo: null,
          showSearchResults: false,
          locale: this.props.pathname.split("/")[1] || "en-US"
        });
      }
    }
  }

  componentWillUnmount() {
    this.dismounted = true;
    document.removeEventListener("keydown", this.focusOnSearchMaybe);
  }

  initializeIndex = () => {
    if (this.state.initializing) return;

    this.setState({ initializing: true }, async () => {
      // Always do the XHR network request (hopefully good HTTP caching
      // will make this pleasant for the client) but localStorage is
      // always faster than XHR even with localStorage's flaws.
      const localStorageCacheKey = `${this.getCurrentLocale()}-titles`;
      const storedTitlesRaw = localStorage.getItem(localStorageCacheKey);
      if (storedTitlesRaw) {
        let storedTitles = null;
        try {
          storedTitles = JSON.parse(storedTitlesRaw);
        } catch (ex) {
          console.warn(ex);
        }
        // XXX Could check the value of 'storedTitles._fetchDate'.
        // For example if `new Date().getTime() - storedTitles._fetchDate`
        // is a really small number, it probably just means the page was
        // refreshed very recently.
        if (storedTitles) {
          this.indexTitles(storedTitles);
        }
      }

      let response;
      try {
        response = await fetch(`/${this.getCurrentLocale()}/titles.json`);
      } catch (ex) {
        if (this.dismounted) return;
        return this.setState({ serverError: ex, showSearchResults: true });
      }
      if (this.dismounted) return;
      if (!response.ok) {
        return this.setState({
          serverError: response,
          showSearchResults: true
        });
      }
      const { titles } = await response.json();
      this.indexTitles(titles);

      // So we can keep track of how old the data is when stored
      // in localStorage.
      titles._fetchDate = new Date().getTime();
      try {
        localStorage.setItem(
          `${this.getCurrentLocale()}-titles`,
          JSON.stringify(titles)
        );
      } catch (ex) {
        console.warn(
          ex,
          `Unable to store a ${JSON.stringify(titles).length} string`
        );
      }
    });
  };

  indexTitles = titles => {
    // NOTE! See search-experimentation.js to play with different settings.
    this.index = new FlexSearch({
      encode: "advanced",
      suggest: true,
      // tokenize: "reverse",
      tokenize: "forward",
      doc: {
        id: "uri",
        field: "title"
      }
    });
    this._map = titles;

    this.index.add(
      Object.entries(this._map).map(([uri, info]) => {
        return { uri, title: info.title, popularity: info.popularity };
      })
    );
    // Object.entries(titles).forEach(([uri, info]) => {
    //   // XXX investigate if it's faster to add all at once
    //   // https://github.com/nextapps-de/flexsearch/#addupdateremove-documents-tofrom-the-index
    //   this.index.add(uri, info);
    // });
    this.fuzzySearcher = new FuzzySearch(Object.keys(this._map));
  };

  searchHandler = event => {
    this.setState({ q: event.target.value }, this.updateSearch);
  };

  updateSearch = () => {
    const q = this.state.q.trim();
    if (!q) {
      if (this.state.showSearchResults) {
        this.setState({ showSearchResults: false });
      }
    } else if (!this.index) {
      // This can happen if the initializing hasn't completed yet or
      // completed un-successfully.
      return;
    } else {
      // console.log(
      //   `Do something interesting with ${this.state.q.trim()} (last search: ${
      //     this.state.lastQ
      //   })`
      // );
      // // XXX we could compare this.state.q and this.state.lastQ
      // // since this.state.lastQ reflects the current results.
      // // IF nothing was found based on this.state.lastQ AND
      // // this.state.q.startsWith(this.state.lastQ)
      // // we know don't need to bother searching again.
      // console.log(
      //   "COMPARE",
      //   [this.state.q, this.state.lastQ],
      //   this.state.searchResults.length
      // );
      // if (
      //   this.state.lastQ &&
      //   this.state.q.startsWith(this.state.lastQ) &&
      //   !this.state.searchResults.length
      // ) {
      //   console.warn(
      //     `Nothing found for ${this.state.lastQ} so don't bother with ${
      //       this.state.q
      //     }`
      //   );
      //   return;
      // }

      // if (this.hideSoon) {
      //   // You have triggered the hideSoon timeout which usually happens
      //   // when you blur the input. But clearly you've changed your mind
      //   // and typed something new interesting in. So cancel that timeout.
      //   window.clearTimeout(this.hideSoon);
      // }

      if (q.startsWith("/") && !/\s/.test(q)) {
        // Fuzzy-String search on the URI

        if (q === "/") {
          this.setState({
            highlitResult: null,
            lastQ: q,
            searchResults: [],
            showSearchResults: true
          });
        } else {
          const fuzzyResults = this.fuzzySearcher.search(q, {
            limit: isMobileUserAgent() ? 5 : 10
          });
          const results = fuzzyResults.map(fuzzyResult => {
            return {
              title: this._map[fuzzyResult.needle].title,
              uri: fuzzyResult.needle,
              substrings: fuzzyResult.substrings
            };
          });
          this.setState({
            highlitResult: results.length ? 0 : null,
            lastQ: q,
            searchResults: results,
            showSearchResults: true
          });
        }
      } else {
        // Full-Text search
        const indexResults = this.index.search({
          query: q,
          field: "title",
          limit: isMobileUserAgent() ? 5 : 10,
          // sort: "popularity",
          sort: (a, b) => b.popularity - a.popularity,
          // bool: "or",
          suggest: true // This can give terrible result suggestions
        });

        const results = indexResults.map(result => {
          return {
            title: this._map[result.uri].title,
            uri: result.uri,

            // temporary
            popularity: this._map[result.uri].popularity
          };
        });
        this.setState({
          highlitResult: results.length ? 0 : null,
          lastQ: q,
          searchResults: results,
          showSearchResults: true
        });
      }
    }
  };

  keyDownHandler = event => {
    if (event.key === "Escape") {
      if (this.state.showSearchResults) {
        this.setState({ showSearchResults: false });
      }
    } else if (event.key === "ArrowDown" || event.key === "Tab") {
      // Increment 'highlitResult' if possible.
      const { highlitResult, searchResults } = this.state;
      if (highlitResult === null) {
        if (searchResults.length) {
          event.preventDefault();
          this.setState({ highlitResult: 0 });
        }
      } else if (highlitResult < searchResults.length - 1) {
        event.preventDefault();
        this.setState({ highlitResult: highlitResult + 1 });
      }
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      // Decrement 'highlitResult' if possible.
      const { highlitResult } = this.state;
      if (highlitResult > 0) {
        this.setState({ highlitResult: highlitResult - 1 });
      } else {
        this.setState({ highlitResult: 0 });
      }
    } else if (event.key === "Tab") {
      // If the user tabbed, only try to control it if there is a good
      // reason to do so.
      // const {highlitResult, searchResults} =this.state;
      // if (highlitResult === null)
      // If the user tabbed, only try to control it if there is a good
      // "background q" to tab-complete to.
      // const backgroundQ = this.computeBackgroundQ(true);
      // if (backgroundQ) {
      //   event.preventDefault();
      //   this.setState({ q: backgroundQ }, this.updateSearch);
      //   // console.warn({ backgroundQ });
      // }
    }
  };

  focusHandler = () => {
    this.inFocus = true;
    this.inputRef.current.placeholder = this.ACTIVE_PLACEHOLDER;

    // If it hasn't been done already, do this now. It's idempotent.
    this.initializeIndex();

    // Perhaps the blur closed the search results
    const { q, searchResults, showSearchResults } = this.state;
    if (!showSearchResults && searchResults.length && q) {
      this.setState({ showSearchResults: true });
    }

    // If you're on a mobile, scroll down a little bit so that the search
    // bar is at the top of your screen. That allows maximum height space
    // usage to fix the input widget, the search result suggestions, and
    // the keyboard.
    if (isMobileUserAgent() && !this._hasScrolledDown) {
      if (this.inputRef.current) {
        this.inputRef.current.scrollIntoView();
      }
      // Don't bother a second time.
      this._hasScrolledDown = true;
    }
  };

  blurHandler = () => {
    this.inFocus = false;
    this.inputRef.current.placeholder = this.INACTIVE_PLACEHOLDER;
    // The reason we have a slight delay before hiding search results
    // is so that any onClick on the results get a chance to fire.
    this.hideSoon = window.setTimeout(() => {
      if (!this.dismounted) {
        this.setState({ showSearchResults: false });
      }
    }, 100);
  };

  submitHandler = event => {
    event.preventDefault();
    const { highlitResult, searchResults } = this.state;
    let redirectTo;
    if (searchResults.length === 1) {
      redirectTo = searchResults[0].uri;
    } else if (searchResults.length && highlitResult !== null) {
      redirectTo = searchResults[highlitResult].uri;
    } else {
      return;
    }
    this.setState({
      redirectTo,
      showSearchResults: false
    });
  };

  redirect = uri => {
    this.setState({ redirectTo: uri });
  };

  // computeBackgroundQ = (caseInsensitive = false) => {
  //   // If there is a good new suggestion, add its full word to 'q'.
  //   const { q, highlitResult, searchResults } = this.state;
  //   if (!q) {
  //     return "";
  //   }
  //   let next;
  //   if (highlitResult !== null) {
  //     next = searchResults[highlitResult].title;
  //   } else if (searchResults.length) {
  //     next = searchResults[0].title;
  //   }

  //   // If the user has typed "jav" and the 'next' value is 'JavaScript'
  //   // we ultimately want to return and suggest 'javaScript'
  //   console.log({ next });
  //   // let regex
  //   if (caseInsensitive) {
  //     return next;
  //   }
  //   if (next) {
  //     return q + next.replace(new RegExp(q, "i"), "");
  //   }

  //   return q;
  // };

  // This exists to avoid having to use 'document.querySelector(...)'
  // to get to the DOM element.
  inputRef = React.createRef();

  render() {
    const {
      highlitResult,
      q,
      redirectTo,
      searchResults,
      serverError,
      showSearchResults
    } = this.state;
    if (redirectTo) {
      return <Redirect noThrow replace={false} to={redirectTo} />;
    }

    // The fuzzy search is engaged if the search term starts with a '/'
    // and does not have any spaces in it.
    const isFuzzySearch = q.startsWith("/") && !/\s/.test(q);

    // Compute this once so it can be used as a conditional
    // and a prop.
    // Nothing found means there was an attempt to find stuff but it
    // came back empty.
    const nothingFound =
      (q && !searchResults.length && !isFuzzySearch) ||
      (isFuzzySearch && q !== "/" && !searchResults.length);

    // This boolean determines if we should bother to show the search
    // results div at all.
    // It's best to know this BEFORE instead of letting
    // the <ShowSearchResults/> component return a null.
    // By knowing it in advance we can use it as a hint to the input widget
    // so it can know to draw a bottom border or not.
    const show =
      !serverError &&
      showSearchResults &&
      (nothingFound || searchResults.length || isFuzzySearch);

    return (
      <form className="search-widget" onSubmit={this.submitHandler}>
        {/* <div className="input-wrapper">
          <input
            type="search"
            className={show ? "background has-search-results" : "background"}
            readOnly
            value={this.computeBackgroundQ()}
          /> */}
        <input
          className={show ? "has-search-results" : null}
          onBlur={this.blurHandler}
          onChange={this.searchHandler}
          onFocus={this.focusHandler}
          onKeyDown={this.keyDownHandler}
          onMouseOver={this.initializeIndex}
          placeholder={this.INACTIVE_PLACEHOLDER}
          ref={this.inputRef}
          type="search"
          value={q}
        />
        {/* </div> */}
        {serverError && (
          <p className="server-error">
            {/* XXX Could be smarter here and actually *look* at the serverError object */}
            Server error trying to initialize index
          </p>
        )}
        {show ? (
          <ShowSearchResults
            highlitResult={highlitResult}
            nothingFound={nothingFound}
            q={q}
            redirect={this.redirect}
            results={searchResults}
            isFuzzySearch={isFuzzySearch}
          />
        ) : null}
      </form>
    );
  }
}

class ShowSearchResults extends React.PureComponent {
  redirectHandler = result => {
    this.props.redirect(result.uri);
  };

  render() {
    const {
      highlitResult,
      isFuzzySearch,
      nothingFound,
      q,
      results
    } = this.props;
    return (
      <div className="search-results">
        {nothingFound && <div className="nothing-found">nothing found</div>}
        {results.map((result, i) => {
          return (
            <div
              className={i === highlitResult ? "highlit" : null}
              key={result.uri}
              onClick={event => {
                this.redirectHandler(result);
              }}
            >
              <small style={{ color: "red" }}>{result.popularity}</small>
              <HighlightMatch title={result.title} q={q} />
              <br />
              <BreadcrumbURI uri={result.uri} substrings={result.substrings} />
            </div>
          );
        })}
        {isFuzzySearch && (
          <div className="fuzzy-engaged">Fuzzy searching by URI</div>
        )}
      </div>
    );
  }
}

function HighlightMatch({ title, q }) {
  // FlexSearch doesn't support finding out which "typo corrections"
  // were done unfortunately.
  // See https://github.com/nextapps-de/flexsearch/issues/99

  // Split on higlight term and include term into parts, ignore case.
  const words = q
    .trim()
    .toLowerCase()
    .split(/[ ,]+/);

  // $& means the whole matched string
  const regexWords = words.map(s => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const regex = `\\b(${regexWords.join("|")})`;
  const parts = title.split(new RegExp(regex, "gi"));
  return (
    <b>
      {parts.map((part, i) => {
        const key = `${part}:${i}`;
        if (words.includes(part.toLowerCase())) {
          return <mark key={key}>{part}</mark>;
        } else {
          return <span key={key}>{part}</span>;
        }
      })}
    </b>
  );
}

function BreadcrumbURI({ uri, substrings }) {
  if (substrings) {
    return (
      <small>
        {substrings.map((part, i) => {
          const key = `${part.str}:${i}`;
          if (part.match) {
            return <mark key={key}>{part.str}</mark>;
          } else {
            return <span key={key}>{part.str}</span>;
          }
        })}
      </small>
    );
  }
  const keep = uri
    .split("/")
    .slice(1)
    .filter(p => p !== "docs");
  return <small>{keep.join(" / ")}</small>;
}
