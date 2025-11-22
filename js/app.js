document.addEventListener('DOMContentLoaded', () => {
    // State
    const state = {
        feed: [],
        sidebar: [],

        filteredFeed: [],
        page: 1,
        itemsPerPage: 10,
        loading: false,
        hasMore: true,
        filters: {
            source: 'all',
            favoritesOnly: false
        },
        userSettings: {
            read: [],
            favorites: [],
            hidden: [],
            theme: 'light',

        },
        currentView: 'home'
    };

    // DOM Elements
    const feedContainer = document.getElementById('feed-container');
    const sidebarContainer = document.getElementById('sidebar-container');
    const navHome = document.getElementById('nav-home');
    const navTech = document.getElementById('nav-tech');
    const viewHome = document.getElementById('view-home');
    const viewTech = document.getElementById('view-tech');

    const sourceFilter = document.getElementById('source-filter');
    const favoritesToggle = document.getElementById('favorites-toggle');

    const themeToggle = document.getElementById('theme-toggle');
    const feedTemplate = document.getElementById('feed-item-template');
    const sidebarTemplate = document.getElementById('sidebar-item-template');

    // Initialization
    init();

    function init() {
        loadSettings();
        applyTheme();

        setupEventListeners();
        fetchData();

        // Initial view
        switchView('home');
    }

    function setupEventListeners() {
        sourceFilter.addEventListener('change', (e) => {
            state.filters.source = e.target.value;
            applyFilters();
        });
        favoritesToggle.addEventListener('change', applyFilters);
        themeToggle.addEventListener('click', toggleTheme);

        // Navigation event listeners
        navHome.addEventListener('click', () => switchView('home'));
        navTech.addEventListener('click', () => {
            switchView('tech');
        });

        // Global scroll listener for side scrolling
        window.addEventListener('wheel', (e) => {
            // Only active in Tech view
            if (state.currentView !== 'tech') return;

            const isOverFeed = e.target.closest('.feed-column');
            const isOverSidebar = e.target.closest('.sidebar-column');

            // If hovering outside content columns (sides)
            if (!isOverFeed && !isOverSidebar) {
                // Scroll both columns
                feedContainer.scrollTop += e.deltaY;
                sidebarContainer.scrollTop += e.deltaY;
            }
        });
    }
    function switchView(viewName) {
        state.currentView = viewName;

        // Update active navigation link
        navHome.classList.toggle('active', viewName === 'home');
        navTech.classList.toggle('active', viewName === 'tech');

        // Show/hide views
        viewHome.classList.toggle('hidden', viewName !== 'home');
        viewTech.classList.toggle('hidden', viewName !== 'tech');

        // Toggle source filter visibility
        // User requested to hide the dropdown on non-Tech pages
        if (sourceFilter) {
            if (viewName === 'tech') {
                sourceFilter.style.display = 'inline-block';
            } else {
                sourceFilter.style.display = 'none';
            }
        }

        // If switching to home, re-apply filters to ensure feed is rendered
        if (viewName === 'home') {
            applyFilters();
        }
    }

    // --- Data Fetching ---

    async function fetchData() {
        try {
            const [feedRes, sidebarRes] = await Promise.allSettled([
                fetch('data/feed.json'),
                fetch('data/sidebar.json')
            ]);

            if (feedRes.status === 'fulfilled' && feedRes.value.ok) {
                try {
                    state.feed = await feedRes.value.json();
                    applyFilters(); // This will trigger render
                    populateSourceFilter();
                } catch (e) {
                    console.error('Error parsing feed:', e);
                    feedContainer.innerHTML = '<div class="loading-indicator">Failed to parse feed.</div>';
                }
            } else {
                console.error('Feed fetch failed or not found');
                feedContainer.innerHTML = '<div class="loading-indicator">Failed to load feed.</div>';
            }

            if (sidebarRes.status === 'fulfilled' && sidebarRes.value.ok) {
                try {
                    state.sidebar = await sidebarRes.value.json();
                    renderSidebar();
                } catch (e) {
                    console.error('Error parsing sidebar:', e);
                    sidebarContainer.innerHTML = '<div class="loading-indicator">Failed to parse updates.</div>';
                }
            } else {
                console.warn('Sidebar fetch failed or not found');
                sidebarContainer.innerHTML = '<div class="loading-indicator">No updates.</div>';
            }



        } catch (error) {
            console.error('Critical error fetching data:', error);
            // Only wipe feed if we haven't successfully loaded it yet
            if (state.feed.length === 0) {
                feedContainer.innerHTML = '<div class="loading-indicator">Error loading data.</div>';
            }
        }
    }

    // --- Rendering ---

    function renderFeed(append = false) {
        if (!append) {
            feedContainer.innerHTML = '';
            state.page = 1;
            state.hasMore = true;
        }

        const start = (state.page - 1) * state.itemsPerPage;
        const end = start + state.itemsPerPage;
        const itemsToRender = state.filteredFeed.slice(start, end);

        if (itemsToRender.length === 0 && !append) {
            feedContainer.innerHTML = '<div class="loading-indicator">No posts found.</div>';
            return;
        }

        if (itemsToRender.length < state.itemsPerPage) {
            state.hasMore = false;
        }

        const fragment = document.createDocumentFragment();

        itemsToRender.forEach(item => {
            const id = item.id || item.link;

            // Skip hidden items
            if (state.userSettings.hidden.includes(id)) return;

            const clone = feedTemplate.content.cloneNode(true);
            const card = clone.querySelector('.feed-card');

            // Mark as read
            if (state.userSettings.read.includes(id)) {
                card.classList.add('read');
            }

            // Content
            clone.querySelector('.source-badge').textContent = item.source;
            clone.querySelector('.date').textContent = formatDate(item.published);

            const titleLink = clone.querySelector('.card-title a');
            titleLink.textContent = item.title;
            titleLink.href = item.link;
            titleLink.onclick = (e) => handleLinkClick(e, id, card);

            clone.querySelector('.card-summary').textContent = item.summary;
            clone.querySelector('.author').textContent = item.author;

            // Image
            if (item.image) {
                const imgWrapper = clone.querySelector('.card-image-wrapper');
                const img = clone.querySelector('.card-image');
                img.src = item.image;
                img.alt = item.title;
                imgWrapper.style.display = 'block';
            }

            // Actions
            const favBtn = clone.querySelector('.favorite-btn');
            if (state.userSettings.favorites.includes(id)) {
                favBtn.classList.add('active');
            }
            favBtn.onclick = () => toggleFavorite(id, favBtn);

            const hideBtn = clone.querySelector('.hide-btn');
            hideBtn.onclick = () => hidePost(id, card);

            const readBtn = clone.querySelector('.read-toggle-btn');
            readBtn.onclick = () => toggleRead(id, card);

            fragment.appendChild(clone);
        });

        feedContainer.appendChild(fragment);

        // Setup intersection observer for infinite scroll on the last item
        setupInfiniteScroll();
    }

    function renderSidebar() {
        sidebarContainer.innerHTML = '';
        if (state.sidebar.length === 0) {
            sidebarContainer.innerHTML = '<div class="loading-indicator">No updates.</div>';
            return;
        }

        const fragment = document.createDocumentFragment();
        state.sidebar.forEach(item => {
            const id = item.link; // Use link as ID for HN items

            // Skip hidden items
            if (state.userSettings.hidden.includes(id)) return;

            const clone = sidebarTemplate.content.cloneNode(true);
            const card = clone.querySelector('.sidebar-card');

            // Mark as read
            if (state.userSettings.read.includes(id)) {
                card.classList.add('read');
            }

            clone.querySelector('.sidebar-date').textContent = formatDate(item.published);
            const titleLink = clone.querySelector('.sidebar-title a');
            titleLink.textContent = item.title;
            titleLink.href = item.link;
            titleLink.onclick = (e) => handleLinkClick(e, id, card);

            // Extract and display source domain
            clone.querySelector('.sidebar-source').textContent = extractDomain(item.link);

            // Actions
            const favBtn = clone.querySelector('.favorite-btn');
            if (state.userSettings.favorites.includes(id)) {
                favBtn.classList.add('active');
            }
            favBtn.onclick = () => toggleFavorite(id, favBtn);

            const hideBtn = clone.querySelector('.hide-btn');
            hideBtn.onclick = () => hidePost(id, card);

            const readBtn = clone.querySelector('.read-toggle-btn');
            readBtn.onclick = () => toggleRead(id, card);

            fragment.appendChild(clone);
        });
        sidebarContainer.appendChild(fragment);
    }

    function setupInfiniteScroll() {
        // Remove existing sentinel if any
        const existingSentinel = document.getElementById('scroll-sentinel');
        if (existingSentinel) existingSentinel.remove();

        if (!state.hasMore) return;

        const sentinel = document.createElement('div');
        sentinel.id = 'scroll-sentinel';
        sentinel.className = 'loading-indicator';
        sentinel.textContent = 'Loading more...';
        feedContainer.appendChild(sentinel);

        const observer = new IntersectionObserver((entries) => {
            if (entries[0].isIntersecting && !state.loading) {
                state.page++;
                renderFeed(true);
            }
        });

        observer.observe(sentinel);
    }

    // --- Logic & Helpers ---

    function applyFilters() {
        state.filteredFeed = state.feed.filter(item => {
            const id = item.id || item.link;

            // Filter by Source
            if (state.filters.source !== 'all' && item.source !== state.filters.source) {
                return false;
            }

            // Filter by Favorites
            if (state.filters.favoritesOnly && !state.userSettings.favorites.includes(id)) {
                return false;
            }

            // Filter Hidden (always applied unless viewing hidden specifically, which isn't a feature yet)
            if (state.userSettings.hidden.includes(id)) {
                return false;
            }

            return true;
        });

        renderFeed(false);
    }

    function populateSourceFilter() {
        const sources = [...new Set(state.feed.map(item => item.source))];
        // Clear existing options except 'All'
        while (sourceFilter.options.length > 1) {
            sourceFilter.remove(1);
        }

        sources.forEach(source => {
            const option = document.createElement('option');
            option.value = source;
            option.textContent = source;
            sourceFilter.appendChild(option);
        });
    }

    function formatDate(isoString) {
        if (!isoString) return '';
        const date = new Date(isoString);
        const now = new Date();
        const diff = now - date;

        // If less than 24 hours, show relative time
        if (diff < 86400000) {
            const hours = Math.floor(diff / 3600000);
            if (hours < 1) return 'Just now';
            return `${hours}h ago`;
        }

        return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
    }

    function extractDomain(url) {
        try {
            const urlObj = new URL(url);
            return urlObj.hostname.replace('www.', '');
        } catch (e) {
            return '';
        }
    }

    // --- User Actions ---

    function handleLinkClick(e, id, card) {
        // Mark as read when clicked
        if (!state.userSettings.read.includes(id)) {
            state.userSettings.read.push(id);
            saveSettings();
            card.classList.add('read');
        }
    }

    function toggleFavorite(id, btn) {
        const index = state.userSettings.favorites.indexOf(id);
        if (index === -1) {
            state.userSettings.favorites.push(id);
            btn.classList.add('active');
        } else {
            state.userSettings.favorites.splice(index, 1);
            btn.classList.remove('active');
            // If we are in favorites only mode, re-render
            if (state.filters.favoritesOnly) {
                applyFilters();
            }
        }
        saveSettings();
    }

    function hidePost(id, card) {
        if (!state.userSettings.hidden.includes(id)) {
            state.userSettings.hidden.push(id);
            saveSettings();
            // Animate removal
            card.style.transform = 'scale(0.9)';
            card.style.opacity = '0';
            setTimeout(() => {
                applyFilters(); // Re-render feed to remove gap
                renderSidebar(); // Re-render sidebar to remove gap
            }, 200);
        }
    }

    function toggleRead(id, card) {
        const index = state.userSettings.read.indexOf(id);
        if (index === -1) {
            state.userSettings.read.push(id);
            card.classList.add('read');
        } else {
            state.userSettings.read.splice(index, 1);
            card.classList.remove('read');
        }
        saveSettings();
    }

    // --- Settings & Theme ---

    function loadSettings() {
        const saved = localStorage.getItem('newsfeed-settings');
        if (saved) {
            state.userSettings = { ...state.userSettings, ...JSON.parse(saved) };
        }
    }

    function saveSettings() {
        localStorage.setItem('newsfeed-settings', JSON.stringify(state.userSettings));
    }

    function applyTheme() {
        document.documentElement.setAttribute('data-theme', state.userSettings.theme);
    }

    function toggleTheme() {
        state.userSettings.theme = state.userSettings.theme === 'light' ? 'dark' : 'light';
        applyTheme();
        saveSettings();
    }




});
