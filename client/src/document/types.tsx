export interface Source {
  github_url: string;
  folder: string;
}

export interface Link {
  href: string;
  line: number;
  column: number;
  suggestion: string | null;
  fixed?: true;
}

type Flaws = {
  broken_links: Link[];
  macros: MacroErrorMessage[];
  bad_bcd_queries: string[];
};

export interface Doc {
  title: string;
  mdn_url: string;
  sidebarHTML: string;
  body: string;
  modified: string;
  flaws: Flaws;
  other_translations?: object[];
  translation_of?: string;
  parents?: Doc[];
  source: Source;
  contributors: string[];
  isArchive: boolean;
}

export interface MacroErrorMessage {
  name: string;
  error: {
    path?: string;
  };
  errorMessage: string;
  line: number;
  column: number;
  filepath: string;
  sourceContext: string;
  macroName: string;
  fixed?: true;
}
