import React, { Component, type ReactNode } from "react";
import ReactDOM from "react-dom/client";
import { App } from "./app/App";
import { store } from "./state/store";
import "./styles/global.css";

// Unhandled promise rejections: expected errors are handled locally; anything
// that slips through is surfaced in the store instead of a silent blank spot
// (TASK 08 §46).
window.addEventListener("unhandledrejection", (event) => {
  store.patch((s) => ({ ...s, lastError: `unhandled rejection: ${String(event.reason ?? "unknown")}` }));
});

// Disable default browser context menu globally to prevent "Print", "Save as", etc.,
// except on input fields where native context menu is expected.
window.addEventListener("contextmenu", (event) => {
  const target = event.target as HTMLElement;
  if (target.tagName !== "INPUT" && target.tagName !== "TEXTAREA") {
    event.preventDefault();
  }
});

// One controlled error boundary: a component throwing must never produce a
// blank, unexplained window (TASK 08 §45). This is NOT a second lifecycle
// authority — it only renders a recovery-safe message.
class ErrorBoundary extends Component<{ children: ReactNode }, { error: string | null }> {
  state: { error: string | null } = { error: null };

  static getDerivedStateFromError(error: unknown): { error: string | null } {
    return { error: String(error) };
  }

  componentDidCatch(error: unknown, info: unknown) {
    store.patch((s) => ({ ...s, lastError: `render error: ${String(error)}` }));
    console.error("SAIWORK2 render error", error, info);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="error-boundary">
          <h1>Something went wrong rendering the UI</h1>
          <p>{this.state.error}</p>
          <button
            className="btn btn--primary"
            onClick={() => {
              this.setState({ error: null });
              store.patch((s) => ({ ...s, lastError: null }));
            }}
          >
            Reload view
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </React.StrictMode>,
);
