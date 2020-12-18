import * as React from "react";

import { useLocale } from "../../../hooks";
import { ReactComponent as LogoSVG } from "../../../assets/logo.svg";

import "./index.scss";

export function Logo({ mode }: { mode?: string }) {
  const locale = useLocale();

  return (
    <a href={`/${locale}/`} className="logo" aria-label="MDN Web Docs">
      <LogoSVG fill={mode === "dark" ? "#fff" : undefined} />
    </a>
  );
}
