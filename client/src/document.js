import React from "react";
import { Link } from "@reach/router";

import { NoMatch } from "./routing";
import { InteractiveExample } from "./ingredients/interactive-example";
import { Attributes } from "./ingredients/attributes";
import { Examples } from "./ingredients/examples";

export class Document extends React.Component {
  state = {
    document: this.props.document || null,
    loading: false,
    notFound: false,
    loadingError: null
  };

  componentDidMount() {
    if (!this.state.document) {
      this.fetchDocument();
    }
  }

  componentDidUpdate(prevProps) {
    const currentSlug = this.props["*"];
    const prevSlug = prevProps["*"];
    if (currentSlug !== prevSlug) {
      this.fetchDocument();
    }
  }

  fetchDocument = () => {
    this.setState({ loading: true }, async () => {
      let url = document.location.pathname;
      if (!url.endsWith("/")) url += "/";
      url += "index.json";
      console.log("OPENING", url);
      let response;
      try {
        response = await fetch(url);
      } catch (ex) {
        return this.setState({ loading: false, loadingError: ex });
      }
      if (!response.ok) {
        console.warn(response);
        return this.setState({ loading: false, loadingError: response });
      } else {
        const data = await response.json();
        document.title = data.document.title;
        this.setState({ document: data.document, loading: false });
      }
    });
  };

  render() {
    const { document, loadingError, loading, notFound } = this.state;
    const { location } = this.props;
    if (notFound) {
      return <NoMatch location={location} message="Document not found" />;
    }
    if (loading) {
      return <p>Loading...</p>;
    }
    if (loadingError) {
      return <LoadingError error={loadingError} />;
    }
    if (!document) {
      return null;
    }
    return (
      <div>
        <h1 className="page-title">{document.title}</h1>
        <div className="main">
          <div className="sidebar">
            {document.legacy ? (
              <RenderLegacySideBar document={document} />
            ) : (
              <RenderSideBar document={document} />
            )}
          </div>
          <div className="content">
            {document.legacy ? (
              <RenderLegacyDocument document={document} />
            ) : (
              <RenderHTMLElementDocument document={document} />
            )}
            <hr />
            {document.contributors && (
              <Contributors contributors={document.contributors} />
            )}
          </div>
        </div>
      </div>
    );
  }
}

function RenderLegacySideBar({ document }) {
  return <div dangerouslySetInnerHTML={{ __html: document.related_content }} />;
}

function RenderSideBar({ document }) {
  return document.related_content.map(node => (
    <SidebarLeaf
      key={node.title}
      depth={0}
      title={node.title}
      content={node.content || []}
    />
  ));
}

function SidebarLeaf({ depth, title, content }) {
  let titleNode;
  if (depth === 0) {
    titleNode = <h3>{title}</h3>;
  } else if (depth === 1) {
    titleNode = <h4>{title}</h4>;
  } else if (depth === 2) {
    titleNode = <h5>{title}</h5>;
  } else if (depth > 2) {
    titleNode = <h6>{title}</h6>;
  }
  return (
    <div>
      {titleNode}
      <ul>
        {content.map(node => {
          if (node.content) {
            return (
              <li key={node.title}>
                <SidebarLeaf
                  depth={depth + 1}
                  title={node.title}
                  content={node.content || []}
                />
              </li>
            );
          } else {
            return (
              <li key={node.uri}>
                <Link to={node.uri}>{node.title}</Link>
              </li>
            );
          }
        })}
      </ul>
    </div>
  );
}

function RenderHTMLElementDocument({ document }) {
  let sections = [];

  sections.push(
    <Prose key="short_description" section={document.prose.short_description} />
  );
  sections.push(
    <InteractiveExample key="interactive_example" document={document} />
  );
  sections.push(<Prose key="overview" section={document.prose.overview} />);
  sections.push(<Attributes key="attributes" document={document} />);
  sections.push(
    <ProseWithHeading key="usage_notes" section={document.prose.usage_notes} />
  );
  sections.push(
    <ProseWithHeading
      key="accessibility_concerns"
      section={document.prose.accessibility_concerns}
    />
  );
  sections.push(<Examples key="examples" document={document} />);
  sections.push(
    <BrowserCompatibility key="browser_compatibility" document={document} />
  );
  sections.push(
    <ProseWithHeading key="see_also" section={document.prose.see_also} />
  );

  return sections;
}

function Prose({ section }) {
  if (!section) {
    return null;
  }
  return <div dangerouslySetInnerHTML={{ __html: section.content }} />;
}

function ProseWithHeading({ id, section }) {
  if (!section) {
    return null;
  }
  return (
    <>
      <h2 id={id}>{section.title}</h2>
      <div dangerouslySetInnerHTML={{ __html: section.content }} />
    </>
  );
}

function Contributors({ contributors }) {
  return (
    <div>
      <b>Contributors to this page:</b>
      <span dangerouslySetInnerHTML={{ __html: contributors }} />
    </div>
  );
}

function BrowserCompatibility({ content }) {
  return (
    <table>
      <thead>
        <tr>
          <th rowSpan={2} />
          <th colSpan={2}>Desktop</th>
          <th colSpan={2}>Mobile</th>
        </tr>
        <tr>
          <th>Chrome</th>
          <th>Edge</th>
          <th>Chrome</th>
          <th>Edge</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>
            <code>video</code>
          </td>
          <td style={{ backgroundColor: "#e4f8e1" }}>3</td>
          <td style={{ backgroundColor: "#e4f8e1" }}>Yes</td>
          <td>?</td>
          <td style={{ backgroundColor: "#f8e1e1" }}>No</td>
        </tr>
      </tbody>
    </table>
  );
}

function LoadingError({ error }) {
  return (
    <div className="loading-error">
      <h3>Loading Error</h3>
      {error instanceof window.Response ? (
        <p>
          <b>{error.status}</b> on <b>{error.url}</b>
          <br />
          <small>{error.statusText}</small>
        </p>
      ) : (
        <p>
          <code>{error.toString()}</code>
        </p>
      )}
    </div>
  );
}

function RenderLegacyDocument({ document }) {
  return <div dangerouslySetInnerHTML={{ __html: document.body }} />;
}
