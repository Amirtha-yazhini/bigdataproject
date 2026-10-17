import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import { LiveProvider } from "./hooks/useLive";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <LiveProvider>
        <App />
      </LiveProvider>
    </BrowserRouter>
  </React.StrictMode>,
);
