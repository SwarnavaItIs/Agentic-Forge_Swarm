import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { Session } from "./Session";
import "./style.css";
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Session>
      <App />
    </Session>
  </StrictMode>,
);
