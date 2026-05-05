const NAV = [
  {
    label: "Start here",
    items: [
      {
        page: "home",
        href: "",
        title: "Overview",
        description: "What Quarry is and why it exists.",
      },
      {
        page: "get-started",
        href: "get-started/",
        title: "Get started",
        description: "Install Quarry and write your first query.",
      },
    ],
  },
  {
    label: "Guides",
    items: [
      {
        page: "dynamic-filters",
        href: "guides/dynamic-filters/",
        title: "Dynamic filters",
        description: "Optional predicates, sorting, and pagination.",
      },
    ],
  },
  {
    label: "Reference",
    items: [
      {
        page: "core",
        href: "reference/core/",
        title: "Core builders",
        description: "Select, Insert, Update, and Delete.",
      },
      {
        page: "identifiers",
        href: "reference/identifiers/",
        title: "Identifiers",
        description: "T, C, and alias helpers for safe quoting.",
      },
      {
        page: "errors",
        href: "reference/errors/",
        title: "Errors",
        description: "Sentinel errors and failure contracts.",
      },
      {
        page: "codex",
        href: "reference/codex/",
        title: "Codex",
        description: "Reusable raw queries and recipes.",
      },
      {
        page: "scan",
        href: "reference/scan/",
        title: "Scan",
        description: "database/sql execution and hydration helpers.",
      },
      {
        page: "dialects",
        href: "reference/dialects/",
        title: "Dialects",
        description: "Placeholder, quoting, and feature policy.",
      },
    ],
  },
  {
    label: "Examples",
    items: [
      {
        page: "examples",
        href: "examples/",
        title: "Examples overview",
        description: "What the example pages cover.",
      },
      {
        page: "search-endpoint",
        href: "examples/search-endpoint/",
        title: "Search endpoint",
        description: "Optional filters, sort keys, and pagination.",
      },
      {
        page: "admin-edit-form",
        href: "examples/admin-edit-form/",
        title: "Admin edit form",
        description: "Patch updates with returning rows.",
      },
      {
        page: "reporting-query",
        href: "examples/reporting-query/",
        title: "Reporting query",
        description: "Aggregates, joins, and raw fragments.",
      },
      {
        page: "feed-list-endpoint",
        href: "examples/feed-list-endpoint/",
        title: "Feed/list endpoint",
        description: "Cursor-style feeds and scan-friendly rows.",
      },
      {
        page: "search-filters",
        href: "examples/search-filters/",
        title: "Search and filters",
        description: "Reusable filter-heavy query shapes.",
      },
      {
        page: "patch-updates",
        href: "examples/patch-updates/",
        title: "Patch updates",
        description: "Update rows without brittle branch logic.",
      },
      {
        page: "raw-recipes",
        href: "examples/raw-recipes/",
        title: "Raw SQL and recipes",
        description: "Escape hatches and reusable queries.",
      },
      {
        page: "scanning",
        href: "examples/scanning/",
        title: "Scanning",
        description: "Hydrate results without an ORM.",
      },
      {
        page: "cross-dialect",
        href: "examples/cross-dialect/",
        title: "Cross-dialect notes",
        description: "What changes across Postgres, MySQL, SQLite.",
      },
    ],
  },
];

const THEME_KEY = "quarry-docs-theme";

const state = {
  base: getSiteBase(),
  themeButton: null,
  searchInput: null,
};

document.addEventListener("DOMContentLoaded", () => {
  applyStoredTheme();
  renderNav();
  bindTopbar();
  enhanceHeadings();
  buildToc();
  enhanceTabs();
  enhanceCodeBlocks();
  setupKeyboardShortcuts();
  setupScrollSpy();
});

function getSiteBase() {
  const script =
    document.currentScript ||
    document.querySelector('script[src*="assets/site.js"]');
  if (!script) {
    return new URL("./", window.location.href).href;
  }
  const url = new URL(script.src, window.location.href);
  return url.href.replace(/\/assets\/site\.js.*$/, "/");
}

function applyStoredTheme() {
  const saved = localStorage.getItem(THEME_KEY);
  const theme = saved || systemTheme();
  setTheme(theme);
}

function systemTheme() {
  return window.matchMedia &&
    window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
}

function setTheme(theme) {
  document.documentElement.dataset.theme = theme;
  localStorage.setItem(THEME_KEY, theme);
  if (state.themeButton) {
    state.themeButton.setAttribute(
      "aria-label",
      theme === "dark" ? "Switch to light theme" : "Switch to dark theme",
    );
    state.themeButton.textContent = theme === "dark" ? "☀" : "☾";
  }
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) {
    meta.content = theme === "dark" ? "#0f172a" : "#2457d6";
  }
}

function toggleTheme() {
  const current = document.documentElement.dataset.theme || systemTheme();
  setTheme(current === "dark" ? "light" : "dark");
}

function bindTopbar() {
  state.themeButton = document.querySelector("[data-theme-toggle]");
  state.searchInput = document.querySelector("[data-docs-search]");
  const menuButton = document.querySelector("[data-menu-toggle]");
  const backdrop = document.querySelector("[data-nav-backdrop]");

  if (state.themeButton) {
    state.themeButton.addEventListener("click", toggleTheme);
  }

  if (menuButton) {
    menuButton.addEventListener("click", () => {
      document.body.classList.toggle("nav-open");
    });
  }

  if (backdrop) {
    backdrop.addEventListener("click", () => {
      document.body.classList.remove("nav-open");
    });
  }

  if (state.searchInput) {
    state.searchInput.addEventListener("input", () => {
      filterNav(state.searchInput.value);
    });
  }

  setTheme(document.documentElement.dataset.theme || systemTheme());
}

function renderNav() {
  const navRoot = document.querySelector("[data-site-nav]");
  if (!navRoot) {
    return;
  }

  const currentPage = document.body.dataset.page || "";
  const fragment = document.createDocumentFragment();

  NAV.forEach((group) => {
    const section = document.createElement("section");
    section.className = "nav-section";

    const heading = document.createElement("h2");
    heading.textContent = group.label;
    section.appendChild(heading);

    const list = document.createElement("div");
    list.className = "nav-list";

    group.items.forEach((item) => {
      const link = document.createElement("a");
      link.className = "nav-link";
      link.href = new URL(item.href, state.base).pathname;
      link.dataset.searchTerm = `${item.title} ${item.description}`.toLowerCase();
      link.dataset.page = item.page;
      if (item.page === currentPage) {
        link.setAttribute("aria-current", "page");
      }

      const title = document.createElement("strong");
      title.textContent = item.title;
      const desc = document.createElement("span");
      desc.textContent = item.description;
      link.append(title, desc);
      list.appendChild(link);
    });

    section.appendChild(list);
    fragment.appendChild(section);
  });

  const empty = document.createElement("div");
  empty.className = "nav-empty";
  empty.hidden = true;
  empty.dataset.navEmpty = "true";
  empty.textContent = "No matching pages. Try a shorter search term.";

  navRoot.innerHTML = "";
  navRoot.append(fragment, empty);
}

function filterNav(query) {
  const navRoot = document.querySelector("[data-site-nav]");
  if (!navRoot) {
    return;
  }
  const q = query.trim().toLowerCase();
  const sections = [...navRoot.querySelectorAll(".nav-section")];
  let matches = 0;

  sections.forEach((section) => {
    const links = [...section.querySelectorAll(".nav-link")];
    let sectionMatches = 0;
    links.forEach((link) => {
      const hit =
        !q || link.dataset.searchTerm.includes(q) || link.textContent.toLowerCase().includes(q);
      link.hidden = !hit;
      if (hit) {
        sectionMatches += 1;
        matches += 1;
      }
    });
    section.hidden = sectionMatches === 0;
  });

  const empty = navRoot.querySelector("[data-nav-empty]");
  if (empty) {
    empty.hidden = matches !== 0;
  }
}

function enhanceHeadings() {
  const content = document.querySelector("[data-doc-content]");
  if (!content) {
    return;
  }

  const used = new Map();
  content.querySelectorAll("h2, h3, h4").forEach((heading) => {
    if (!heading.id) {
      heading.id = uniqueSlug(heading.textContent || "section", used);
    }
    const anchor = document.createElement("a");
    anchor.className = "heading-anchor";
    anchor.href = `#${heading.id}`;
    anchor.setAttribute("aria-label", `Link to ${heading.textContent}`);
    anchor.textContent = "§";
    heading.appendChild(anchor);
  });
}

function buildToc() {
  const toc = document.querySelector("[data-page-toc]");
  if (!toc) {
    return;
  }

  const content = document.querySelector("[data-doc-content]");
  if (!content) {
    toc.remove();
    return;
  }

  const headings = [...content.querySelectorAll("h2, h3, h4")];
  if (!headings.length) {
    toc.remove();
    return;
  }

  const title = document.createElement("h2");
  title.textContent = "On this page";

  const list = document.createElement("ul");
  list.className = "toc-list";

  headings.forEach((heading) => {
    const item = document.createElement("li");
    const link = document.createElement("a");
    link.className = "toc-link";
    link.href = `#${heading.id}`;
    link.dataset.target = heading.id;
    link.dataset.depth = heading.tagName.slice(1);
    link.textContent = heading.textContent.replace("§", "").trim();
    item.appendChild(link);
    list.appendChild(item);
  });

  toc.innerHTML = "";
  toc.append(title, list);
}

function setupScrollSpy() {
  const tocLinks = [...document.querySelectorAll(".toc-link")];
  const headings = tocLinks
    .map((link) => document.getElementById(link.dataset.target))
    .filter(Boolean);

  if (!tocLinks.length || !headings.length || !("IntersectionObserver" in window)) {
    return;
  }

  const activate = (id) => {
    tocLinks.forEach((link) => {
      link.classList.toggle("active", link.dataset.target === id);
    });
  };

  const observer = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((a, b) => b.intersectionRatio - a.intersectionRatio);
      if (visible[0]) {
        activate(visible[0].target.id);
      }
    },
    {
      rootMargin: "-20% 0px -70% 0px",
      threshold: [0.15, 0.35, 0.5],
    },
  );

  headings.forEach((heading) => observer.observe(heading));
}

function enhanceTabs() {
  document.querySelectorAll("[data-tabs]").forEach((group) => {
    const buttons = [...group.querySelectorAll("[data-tab-button]")];
    const panels = [...group.querySelectorAll("[data-tab-panel]")];
    if (!buttons.length || !panels.length) {
      return;
    }

    const activate = (key) => {
      buttons.forEach((button) => {
        const active = button.dataset.tabButton === key;
        button.setAttribute("aria-selected", String(active));
      });
      panels.forEach((panel) => {
        panel.classList.toggle("active", panel.dataset.tabPanel === key);
      });
    };

    buttons.forEach((button) => {
      button.addEventListener("click", () => activate(button.dataset.tabButton));
    });

    activate(buttons[0].dataset.tabButton);
  });
}

function enhanceCodeBlocks() {
  document.querySelectorAll("pre").forEach((pre) => {
    if (pre.querySelector(".copy-button")) {
      return;
    }
    const button = document.createElement("button");
    button.className = "copy-button";
    button.type = "button";
    button.textContent = "Copy";
    button.addEventListener("click", async () => {
      const code = pre.querySelector("code");
      const text = code ? code.textContent : pre.textContent;
      try {
        await navigator.clipboard.writeText(text.trimEnd());
        button.textContent = "Copied";
        window.setTimeout(() => {
          button.textContent = "Copy";
        }, 1300);
      } catch {
        button.textContent = "Failed";
        window.setTimeout(() => {
          button.textContent = "Copy";
        }, 1300);
      }
    });
    pre.appendChild(button);
  });
}

function setupKeyboardShortcuts() {
  window.addEventListener("keydown", (event) => {
    if (event.key === "/" && !isTypingTarget(event.target)) {
      event.preventDefault();
      state.searchInput?.focus();
      return;
    }
    if (event.key === "Escape") {
      document.body.classList.remove("nav-open");
      state.searchInput?.blur();
    }
  });
}

function isTypingTarget(target) {
  if (!(target instanceof HTMLElement)) {
    return false;
  }
  const tag = target.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || target.isContentEditable;
}

function uniqueSlug(text, used) {
  const base = slugify(text);
  const count = used.get(base) || 0;
  used.set(base, count + 1);
  return count === 0 ? base : `${base}-${count + 1}`;
}

function slugify(text) {
  return text
    .toLowerCase()
    .replace(/['"]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-");
}
