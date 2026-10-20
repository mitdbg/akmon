import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { HomeApp } from "./operator-ui/app/HomeApp";
import { initTheme } from "./operator-ui/styles/theme";
import "./operator-ui/styles/tokens.css";
import "./operator-ui/styles/primitives.css";

// Set the firing (light/dark) on the document root before the first paint so
// there is no light-to-dark flash.
initTheme();

const root = document.getElementById("root");

if (!root) {
  throw new Error("OKG operator UI root element was not found");
}

createRoot(root).render(
  <StrictMode>
    <HomeApp />
  </StrictMode>,
);
