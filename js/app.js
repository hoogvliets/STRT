document.addEventListener('DOMContentLoaded', () => {
    // State
    const state = {
        tech: [],
        news: [],
        ball: [],
        sidebar: [],
        widgets: [], // Array of widget objects

        // Active context
        get activeFeed() {
            if (this.currentView === 'news') return this.news;
            if (this.currentView === 'ball') return this.ball;
            return this.tech;
        },

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
        currentView: 'home',
        currentWidgetId: null, // For tracking which widget is being edited
        draggedWidget: null // For drag and drop
    };

    // DOM Elements
    const feedContainer = document.getElementById('feed-container');
    const sidebarContainer = document.getElementById('sidebar-container');
    const newsFeedContainer = document.getElementById('news-feed-container');
    const newsSidebarContainer = document.getElementById('news-sidebar-container');
    const ballFeedContainer = document.getElementById('ball-feed-container');
    const ballSidebarContainer = document.getElementById('ball-sidebar-container');

    const navHome = document.getElementById('nav-home');
    const navTech = document.getElementById('nav-tech');
    const navNews = document.getElementById('nav-news');
    const navBall = document.getElementById('nav-ball');
    const viewHome = document.getElementById('view-home');
    const viewTech = document.getElementById('view-tech');
    const viewNews = document.getElementById('view-news');
    const viewBall = document.getElementById('view-ball');

    const sourceFilter = document.getElementById('source-filter');
    const favoritesToggle = document.getElementById('favorites-toggle');

    const themeToggle = document.getElementById('theme-toggle');
    const feedTemplate = document.getElementById('feed-item-template');
    const sidebarTemplate = document.getElementById('sidebar-item-template');

    // Widget elements
    const widgetsGrid = document.getElementById('widgets-grid');
    const widgetModal = document.getElementById('widget-modal');
    const linkModal = document.getElementById('link-modal');
    const widgetTypeSelect = document.getElementById('widget-type');
    const widgetTitleInput = document.getElementById('widget-title');
    const widgetLocationInput = document.getElementById('widget-location');
    const weatherConfig = document.getElementById('weather-config');
    const saveWidgetBtn = document.getElementById('save-widget-btn');
    const cancelWidgetBtn = document.getElementById('cancel-widget-btn');
    const linkTitleInput = document.getElementById('link-title');
    const linkUrlInput = document.getElementById('link-url');
    const saveLinkBtn = document.getElementById('save-link-btn');
    const cancelLinkBtn = document.getElementById('cancel-link-btn');

    // Initially hide weather config
    if (weatherConfig) {
        weatherConfig.classList.add('hidden');
    }

    // Widget templates
    const bookmarksTemplate = document.getElementById('widget-bookmarks-template');
    const launchpadTemplate = document.getElementById('widget-launchpad-template');
    const notesTemplate = document.getElementById('widget-notes-template');
    const weatherTemplate = document.getElementById('widget-weather-template');
    const todoTemplate = document.getElementById('widget-todo-template');
    const bookmarkLinkTemplate = document.getElementById('bookmark-link-template');
    const launchpadItemTemplate = document.getElementById('launchpad-item-template');
    const weatherHourlyTemplate = document.getElementById('weather-hourly-item-template');
    const todoItemTemplate = document.getElementById('todo-item-template');

    // Initialization
    init();

    function init() {
        loadSettings();
        loadWidgets();
        applyTheme();

        setupEventListeners();
        setupWidgetListeners();
        fetchData();
        renderWidgets();

        // Initial view
        switchView('home');
    }

    function setupEventListeners() {
        sourceFilter.addEventListener('change', (e) => {
            state.filters.source = e.target.value;
            applyFilters();
        });
        favoritesToggle.addEventListener('click', () => {
            state.filters.favoritesOnly = !state.filters.favoritesOnly;
            favoritesToggle.classList.toggle('active', state.filters.favoritesOnly);
            applyFilters();
        });
        themeToggle.addEventListener('click', toggleTheme);

        // Navigation event listeners
        // Navigation event listeners
        if (navHome) {
            navHome.addEventListener('click', () => {
                console.log('Home clicked');
                switchView('home');
            });
        } else console.error('navHome not found');

        if (navTech) {
            navTech.addEventListener('click', () => {
                console.log('Tech clicked');
                switchView('tech');
            });
        } else console.error('navTech not found');

        if (navNews) {
            navNews.addEventListener('click', () => {
                console.log('News clicked');
                switchView('news');
            });
        } else console.error('navNews not found');

        if (navBall) {
            navBall.addEventListener('click', () => {
                console.log('Ball clicked');
                switchView('ball');
            });
        } else console.error('navBall not found');

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

    function setupWidgetListeners() {
        // Widget type change
        widgetTypeSelect.addEventListener('change', (e) => {
            if (e.target.value === 'weather') {
                weatherConfig.classList.remove('hidden');
            } else {
                weatherConfig.classList.add('hidden');
            }
        });

        // Save widget
        saveWidgetBtn.addEventListener('click', saveWidget);
        cancelWidgetBtn.addEventListener('click', closeWidgetModal);

        // Save link
        saveLinkBtn.addEventListener('click', saveLink);
        cancelLinkBtn.addEventListener('click', closeLinkModal);

        // Close modal on clicking outside or X button
        document.querySelectorAll('.modal-close').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const modal = e.target.closest('.modal');
                modal.classList.add('hidden');
            });
        });

        window.addEventListener('click', (e) => {
            if (e.target.classList.contains('modal')) {
                e.target.classList.add('hidden');
            }
        });
    }
    function switchView(viewName) {
        console.log('switchView called:', viewName);
        state.currentView = viewName;

        // Update active navigation link
        navHome.classList.toggle('active', viewName === 'home');
        navTech.classList.toggle('active', viewName === 'tech');
        navNews.classList.toggle('active', viewName === 'news');
        navBall.classList.toggle('active', viewName === 'ball');

        // Show/hide all views
        const viewConfig = document.getElementById('view-config');
        viewHome.classList.toggle('hidden', viewName !== 'home');
        viewTech.classList.toggle('hidden', viewName !== 'tech');
        viewNews.classList.toggle('hidden', viewName !== 'news');
        viewBall.classList.toggle('hidden', viewName !== 'ball');
        if (viewConfig) {
            viewConfig.classList.toggle('hidden', viewName !== 'config');
        }

        // Toggle source filter visibility
        if (sourceFilter) {
            if (viewName === 'tech' || viewName === 'news' || viewName === 'ball') {
                sourceFilter.style.display = 'inline-block';
                // Reset and populate filter for the new view
                state.filters.source = 'all';
                sourceFilter.value = 'all';
                populateSourceFilter();
                applyFilters(); // Re-render feed when switching views
            } else {
                sourceFilter.style.display = 'none';
            }
        }
    }

    // --- Data Fetching ---

    // --- RSS Feed Loading Functions ---

    /**
     * Load feed URLs from YAML config file
     */
    async function loadFeedConfig(configPath) {
        try {
            const response = await fetch(configPath);
            if (!response.ok) {
                console.error(`Failed to load config ${configPath}: HTTP ${response.status}`);
                return [];
            }
            const yamlText = await response.text();
            const config = jsyaml.load(yamlText);
            return config.feeds || [];
        } catch (error) {
            console.error(`Failed to load config ${configPath}:`, error);
            return [];
        }
    }

    /**
     * Get cached feed data from LocalStorage
     */
    function getCachedFeed(cacheKey, ttl) {
        try {
            const cached = localStorage.getItem(cacheKey);
            if (!cached) return null;

            const { data, timestamp } = JSON.parse(cached);
            const age = Date.now() - timestamp;

            if (age < ttl) {
                console.log(`Cache hit: ${cacheKey} (age: ${Math.round(age / 1000)}s)`);
                return data;
            }

            // Expired - remove from cache
            localStorage.removeItem(cacheKey);
            return null;
        } catch (error) {
            console.error('Cache read error:', error);
            return null;
        }
    }

    /**
     * Save feed data to LocalStorage cache
     */
    function cacheFeed(cacheKey, data) {
        try {
            const cacheEntry = {
                data,
                timestamp: Date.now()
            };
            localStorage.setItem(cacheKey, JSON.stringify(cacheEntry));
        } catch (error) {
            // LocalStorage full - clear old feed cache entries
            console.warn('LocalStorage full, clearing feed cache');
            clearFeedCache();

            // Try again after clearing
            try {
                const cacheEntry = { data, timestamp: Date.now() };
                localStorage.setItem(cacheKey, JSON.stringify(cacheEntry));
            } catch (retryError) {
                console.error('Failed to cache feed even after clearing:', retryError);
            }
        }
    }

    /**
     * Clear all feed cache entries from LocalStorage
     */
    function clearFeedCache() {
        const keys = Object.keys(localStorage);
        keys.forEach(key => {
            if (key.startsWith('feed_')) {
                localStorage.removeItem(key);
            }
        });
        console.log('Feed cache cleared');
    }

    /**
     * Fetch a single RSS feed with caching
     */
    async function fetchSingleFeed(feedUrl, cacheTTL = 1800000) {
        // Check cache first (30 min default TTL)
        const cacheKey = `feed_${feedUrl}`;
        const cached = getCachedFeed(cacheKey, cacheTTL);
        if (cached) return cached;

        // Use corsproxy.io CORS proxy to bypass CORS restrictions
        const proxyUrl = `https://corsproxy.io/?${encodeURIComponent(feedUrl)}`;

        // Fetch with timeout
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 20000); // 20s timeout

            const response = await fetch(proxyUrl, {
                signal: controller.signal
            });
            clearTimeout(timeoutId);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const xmlText = await response.text();

            // Parse RSS XML using rss-parser.js
            const posts = parseRSSFeed(xmlText, feedUrl);

            // Cache result
            cacheFeed(cacheKey, posts);

            console.log(`Fetched ${posts.length} posts from ${feedUrl}`);
            return posts;

        } catch (error) {
            // Handle different error types
            if (error.name === 'AbortError') {
                console.warn(`Timeout fetching ${feedUrl} (proxy may be slow)`);
            } else {
                console.error(`Failed to fetch ${feedUrl}:`, error);
            }

            return []; // Graceful degradation
        }
    }

    /**
     * Deduplicate posts by link
     */
    function deduplicatePosts(posts) {
        const seen = new Set();
        return posts.filter(post => {
            if (seen.has(post.link)) return false;
            seen.add(post.link);
            return true;
        });
    }

    /**
     * Fetch all feeds for a category and process them
     */
    async function fetchAllFeeds(feedUrls, category) {
        if (!feedUrls || feedUrls.length === 0) {
            console.log(`No feeds configured for ${category}`);
            return [];
        }

        console.log(`Fetching ${feedUrls.length} feeds for ${category}...`);

        // Fetch all feeds in parallel (Promise.allSettled for resilience)
        const results = await Promise.allSettled(
            feedUrls.map(url => fetchSingleFeed(url))
        );

        // Flatten results, filter out failed feeds
        const allPosts = results
            .filter(r => r.status === 'fulfilled')
            .flatMap(r => r.value);

        // Deduplicate by link
        const uniquePosts = deduplicatePosts(allPosts);

        // Sort by date (newest first)
        uniquePosts.sort((a, b) => {
            const dateA = new Date(a.published || 0);
            const dateB = new Date(b.published || 0);
            return dateB - dateA;
        });

        // Apply 60-day retention
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - 60);

        const filtered = uniquePosts.filter(post => {
            const postDate = new Date(post.published);
            return postDate >= cutoffDate;
        });

        console.log(`${category}: ${filtered.length} posts after deduplication and retention`);
        return filtered;
    }

    /**
     * Fetch Hacker News feed
     */
    async function fetchHackerNews() {
        const url = 'https://news.ycombinator.com/rss';

        try {
            const posts = await fetchSingleFeed(url);

            // Sort by date (newest first)
            posts.sort((a, b) => {
                const dateA = new Date(a.published || 0);
                const dateB = new Date(b.published || 0);
                return dateB - dateA;
            });

            // HN RSS returns full content, limit to 20
            return posts.slice(0, 20).map(post => ({
                ...post,
                source: 'Hacker News',
                author: 'Hacker News'
            }));
        } catch (error) {
            console.error('Failed to fetch Hacker News:', error);
            return [];
        }
    }

    // --- End RSS Feed Loading Functions ---

    async function fetchData() {
        console.log('fetchData called - loading configs and RSS feeds');

        try {
            // Load YAML configs in parallel
            const [techFeeds, newsFeeds, ballFeeds] = await Promise.all([
                loadFeedConfig('config/tech-feed.yaml'),
                loadFeedConfig('config/news-feed.yaml'),
                loadFeedConfig('config/ball-feed.yaml')
            ]);

            console.log('Config loaded:', {
                tech: techFeeds.length,
                news: newsFeeds.length,
                ball: ballFeeds.length
            });

            // Fetch and parse RSS feeds in parallel
            const [techPosts, newsPosts, ballPosts, sidebarPosts] = await Promise.all([
                fetchAllFeeds(techFeeds, 'tech'),
                fetchAllFeeds(newsFeeds, 'news'),
                fetchAllFeeds(ballFeeds, 'ball'),
                fetchHackerNews()
            ]);

            // Update state
            state.tech = techPosts;
            state.news = newsPosts;
            state.ball = ballPosts;
            state.sidebar = sidebarPosts;

            console.log('Feeds loaded:', {
                tech: state.tech.length,
                news: state.news.length,
                ball: state.ball.length,
                sidebar: state.sidebar.length
            });

            // Initial render if we are on a feed view (though init starts at home)
            if (state.currentView !== 'home') {
                console.log('Initial render for view:', state.currentView);
                applyFilters();
                populateSourceFilter();
            } else {
                console.log('Initial view is home, skipping feed render');
            }

            // Render sidebar
            renderSidebar();

        } catch (error) {
            console.error('Critical error fetching data:', error);
        }
    }

    // --- Rendering ---

    function renderFeed(append = false) {
        console.log('renderFeed called', { view: state.currentView, append });
        let container;
        if (state.currentView === 'news') container = newsFeedContainer;
        else if (state.currentView === 'ball') container = ballFeedContainer;
        else container = feedContainer;

        if (!container) {
            console.error('Container not found for view:', state.currentView);
            return;
        }

        if (!append) {
            container.innerHTML = '';
            state.page = 1;
            state.hasMore = true;
        }

        const start = (state.page - 1) * state.itemsPerPage;
        const end = start + state.itemsPerPage;
        const itemsToRender = state.filteredFeed.slice(start, end);
        console.log('Items to render:', itemsToRender.length);

        if (itemsToRender.length === 0 && !append) {
            container.innerHTML = '<div class="loading-indicator">No posts found.</div>';
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

            clone.querySelector('.card-summary').textContent = stripHtml(item.summary);
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

        container.appendChild(fragment);

        // Setup intersection observer for infinite scroll on the last item
        setupInfiniteScroll();
    }

    function renderSidebar() {
        [sidebarContainer, newsSidebarContainer, ballSidebarContainer].forEach(container => {
            if (!container) return;
            container.innerHTML = '';
            if (state.sidebar.length === 0) {
                container.innerHTML = '<div class="loading-indicator">No updates.</div>';
                return;
            }
        });

        if (state.sidebar.length === 0) return;

        // Create fragments for each container
        // ... (omitted loop logic for brevity, same as before but applied to all containers)

        [sidebarContainer, newsSidebarContainer, ballSidebarContainer].forEach(container => {
            if (!container) return;
            const fragment = document.createDocumentFragment();
            state.sidebar.forEach(item => {
                const id = item.link;
                if (state.userSettings.hidden.includes(id)) return;

                const clone = sidebarTemplate.content.cloneNode(true);
                const card = clone.querySelector('.sidebar-card');

                if (state.userSettings.read.includes(id)) card.classList.add('read');

                clone.querySelector('.sidebar-date').textContent = formatDate(item.published);
                const titleLink = clone.querySelector('.sidebar-title a');
                titleLink.textContent = item.title;
                titleLink.href = item.link;
                titleLink.onclick = (e) => handleLinkClick(e, id, card);

                clone.querySelector('.sidebar-source').textContent = extractDomain(item.link);

                const favBtn = clone.querySelector('.favorite-btn');
                if (state.userSettings.favorites.includes(id)) favBtn.classList.add('active');
                favBtn.onclick = () => toggleFavorite(id, favBtn);

                const hideBtn = clone.querySelector('.hide-btn');
                hideBtn.onclick = () => hidePost(id, card);

                const readBtn = clone.querySelector('.read-toggle-btn');
                readBtn.onclick = () => toggleRead(id, card);

                fragment.appendChild(clone);
            });
            container.appendChild(fragment);
        });
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
        let container;
        if (state.currentView === 'news') container = newsFeedContainer;
        else if (state.currentView === 'ball') container = ballFeedContainer;
        else container = feedContainer;

        container.appendChild(sentinel);

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
        state.filteredFeed = state.activeFeed.filter(item => {
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
        const sources = [...new Set(state.activeFeed.map(item => item.source))];
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

    function stripHtml(html) {
        if (!html) return '';
        const tmp = document.createElement('DIV');
        tmp.innerHTML = html;
        return tmp.textContent || tmp.innerText || '';
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

    // --- Widget Management ---

    function loadWidgets() {
        const saved = localStorage.getItem('widgets-data');
        if (saved) {
            try {
                state.widgets = JSON.parse(saved);
                console.log('Loaded widgets:', state.widgets);
            } catch (error) {
                console.error('Error loading widgets:', error);
                state.widgets = [];
            }
        }
    }

    function saveWidgets() {
        localStorage.setItem('widgets-data', JSON.stringify(state.widgets));
    }

    function openWidgetModal() {
        widgetModal.classList.remove('hidden');
        widgetTitleInput.value = '';
        widgetLocationInput.value = '';
        widgetTypeSelect.value = 'bookmarks';
        if (weatherConfig) {
            weatherConfig.classList.add('hidden');
        }
    }

    function closeWidgetModal() {
        widgetModal.classList.add('hidden');
    }

    function saveWidget() {
        const type = widgetTypeSelect.value;
        const title = widgetTitleInput.value.trim();
        const location = widgetLocationInput.value.trim();

        if (!title) {
            alert('Please enter a widget title');
            return;
        }

        // Validate location for weather widget
        if (type === 'weather' && !location) {
            alert('Please enter a location for the weather widget');
            return;
        }

        const widget = {
            id: Date.now().toString(),
            type,
            title,
            location: type === 'weather' ? location : null,
            links: type === 'bookmarks' ? [] : null,
            sites: type === 'launchpad' ? [] : null,
            notes: type === 'notes' ? '' : null,
            todos: type === 'todo' ? [] : null
        };

        state.widgets.push(widget);
        saveWidgets();
        renderWidgets();
        closeWidgetModal();

        // Fetch weather if needed
        if (type === 'weather' && location) {
            fetchWeather(widget.id, location);
        }
    }

    function deleteWidget(widgetId) {
        if (confirm('Are you sure you want to delete this widget?')) {
            state.widgets = state.widgets.filter(w => w.id !== widgetId);
            saveWidgets();
            renderWidgets();
        }
    }

    function openLinkModal(widgetId) {
        state.currentWidgetId = widgetId;
        const widget = state.widgets.find(w => w.id === widgetId);
        const modalTitle = document.getElementById('link-modal-title');
        const linkTitleLabel = linkTitleInput.previousElementSibling;

        if (widget && widget.type === 'launchpad') {
            modalTitle.textContent = 'Add Site';
            // Hide title field for launchpad
            linkTitleLabel.style.display = 'none';
            linkTitleInput.style.display = 'none';
        } else {
            modalTitle.textContent = 'Add Link';
            // Show title field for bookmarks
            linkTitleLabel.style.display = 'block';
            linkTitleInput.style.display = 'block';
        }
        linkModal.classList.remove('hidden');
        linkTitleInput.value = '';
        linkUrlInput.value = '';
    }

    function closeLinkModal() {
        linkModal.classList.add('hidden');
        state.currentWidgetId = null;
    }

    function saveLink() {
        const widget = state.widgets.find(w => w.id === state.currentWidgetId);
        const title = linkTitleInput.value.trim();
        let url = linkUrlInput.value.trim();

        // For launchpad, only URL is required
        if (widget && widget.type === 'launchpad') {
            if (!url) {
                alert('Please enter a URL');
                return;
            }
        } else {
            // For bookmarks, both title and URL are required
            if (!title || !url) {
                alert('Please enter both title and URL');
                return;
            }
        }

        // Ensure URL has a protocol
        if (!url.match(/^https?:\/\//i)) {
            url = 'https://' + url;
        }

        if (widget) {
            if (widget.links) {
                widget.links.push({ id: Date.now().toString(), title, url });
            } else if (widget.sites) {
                // For launchpad, use domain as title fallback
                const urlObj = new URL(url);
                const domain = urlObj.hostname.replace('www.', '');
                widget.sites.push({ id: Date.now().toString(), title: title || domain, url });
            }
            saveWidgets();
            renderWidgets();
            closeLinkModal();
        }
    }

    function deleteLink(widgetId, linkId) {
        const widget = state.widgets.find(w => w.id === widgetId);
        if (widget) {
            if (widget.links) {
                widget.links = widget.links.filter(l => l.id !== linkId);
            } else if (widget.sites) {
                widget.sites = widget.sites.filter(s => s.id !== linkId);
            }
            saveWidgets();
            renderWidgets();
        }
    }

    function updateNotes(widgetId, notes) {
        const widget = state.widgets.find(w => w.id === widgetId);
        if (widget) {
            widget.notes = notes;
            saveWidgets();
        }
    }

    function addTodo(widgetId) {
        const todoText = prompt('Enter task:');
        if (todoText && todoText.trim()) {
            const widget = state.widgets.find(w => w.id === widgetId);
            if (widget && widget.todos) {
                const todo = {
                    id: Date.now().toString(),
                    text: todoText.trim(),
                    completed: false
                };
                widget.todos.push(todo);
                saveWidgets();
                renderWidgets();
            }
        }
    }

    function toggleTodo(widgetId, todoId) {
        const widget = state.widgets.find(w => w.id === widgetId);
        if (widget && widget.todos) {
            const todo = widget.todos.find(t => t.id === todoId);
            if (todo) {
                todo.completed = !todo.completed;
                saveWidgets();
                renderWidgets();
            }
        }
    }

    function deleteTodo(widgetId, todoId) {
        const widget = state.widgets.find(w => w.id === widgetId);
        if (widget && widget.todos) {
            widget.todos = widget.todos.filter(t => t.id !== todoId);
            saveWidgets();
            renderWidgets();
        }
    }

    function renderWidgets() {
        console.log('renderWidgets called, widgets count:', state.widgets.length);
        if (!widgetsGrid) {
            console.error('widgetsGrid element not found');
            return;
        }

        widgetsGrid.innerHTML = '';

        state.widgets.forEach(widget => {
            let template, clone, widgetEl;

            try {
                if (widget.type === 'bookmarks') {
                    clone = bookmarksTemplate.content.cloneNode(true);
                    widgetEl = clone.querySelector('.widget');
                    widgetEl.dataset.widgetId = widget.id;

                    clone.querySelector('.widget-title').textContent = widget.title;

                    // Render links
                    const linksList = clone.querySelector('.bookmarks-list');
                    if (widget.links && widget.links.length > 0) {
                        widget.links.forEach(link => {
                            const linkClone = bookmarkLinkTemplate.content.cloneNode(true);
                            const linkEl = linkClone.querySelector('.bookmark-link');
                            linkEl.dataset.linkId = link.id;

                            const anchor = linkClone.querySelector('.bookmark-link-text');
                            anchor.textContent = link.title;
                            anchor.href = link.url;

                            // Add favicon
                            const favicon = linkClone.querySelector('.bookmark-favicon');
                            const faviconUrl = getFaviconUrl(link.url);
                            favicon.src = faviconUrl;
                            favicon.onerror = () => {
                                // Fallback to a generic link icon if favicon fails to load
                                favicon.style.display = 'none';
                            };

                            const deleteBtn = linkClone.querySelector('.link-delete-btn');
                            deleteBtn.onclick = () => deleteLink(widget.id, link.id);

                            linksList.appendChild(linkClone);
                        });
                    } else {
                        linksList.innerHTML = '<div class="empty-links">No links yet</div>';
                    }

                    // Add link button
                    const addLinkBtn = clone.querySelector('.add-link-btn');
                    addLinkBtn.onclick = () => openLinkModal(widget.id);

                    // Edit title button
                    const editTitleBtn = clone.querySelector('.edit-title-btn');
                    editTitleBtn.onclick = () => editWidgetTitle(widget.id);

                } else if (widget.type === 'launchpad') {
                    clone = launchpadTemplate.content.cloneNode(true);
                    widgetEl = clone.querySelector('.widget');
                    widgetEl.dataset.widgetId = widget.id;

                    clone.querySelector('.widget-title').textContent = widget.title;

                    // Render sites in 3x3 grid
                    const launchpadGrid = clone.querySelector('.launchpad-grid');
                    if (widget.sites && widget.sites.length > 0) {
                        widget.sites.forEach(site => {
                            const siteClone = launchpadItemTemplate.content.cloneNode(true);
                            const siteEl = siteClone.querySelector('.launchpad-item');

                            siteEl.href = site.url;

                            // Add large favicon
                            const favicon = siteClone.querySelector('.launchpad-favicon');
                            const faviconUrl = getLargeFaviconUrl(site.url);
                            favicon.src = faviconUrl;
                            favicon.alt = site.title;
                            favicon.onerror = () => {
                                // Fallback to default icon
                                favicon.style.display = 'none';
                                const iconDiv = siteClone.querySelector('.launchpad-icon');
                                iconDiv.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path>
                                <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path>
                            </svg>`;
                            };

                            const deleteBtn = siteClone.querySelector('.launchpad-delete-btn');
                            deleteBtn.onclick = (e) => {
                                e.preventDefault();
                                deleteLink(widget.id, site.id);
                            };

                            launchpadGrid.appendChild(siteClone);
                        });
                    } else {
                        launchpadGrid.innerHTML = '<div class="empty-launchpad">No sites yet</div>';
                    }

                    // Add site button
                    const addSiteBtn = clone.querySelector('.add-site-btn');
                    addSiteBtn.onclick = () => openLinkModal(widget.id);

                    // Edit title button
                    const editTitleBtn2 = clone.querySelector('.edit-title-btn');
                    editTitleBtn2.onclick = () => editWidgetTitle(widget.id);

                } else if (widget.type === 'notes') {
                    clone = notesTemplate.content.cloneNode(true);
                    widgetEl = clone.querySelector('.widget');
                    widgetEl.dataset.widgetId = widget.id;

                    clone.querySelector('.widget-title').textContent = widget.title;

                    const textarea = clone.querySelector('.notes-textarea');
                    textarea.value = widget.notes || '';

                    // Auto-resize textarea after it's added to DOM
                    textarea.addEventListener('input', (e) => {
                        updateNotes(widget.id, e.target.value);
                        autoResizeTextarea(e.target);
                    });

                    // Edit title button
                    const editTitleBtn = clone.querySelector('.edit-title-btn');
                    editTitleBtn.onclick = () => editWidgetTitle(widget.id);

                } else if (widget.type === 'weather') {
                    clone = weatherTemplate.content.cloneNode(true);
                    widgetEl = clone.querySelector('.widget');
                    widgetEl.dataset.widgetId = widget.id;

                    clone.querySelector('.widget-title').textContent = widget.title;

                    const locationEl = clone.querySelector('.weather-location');
                    const currentTempEl = clone.querySelector('.weather-current-temp');
                    const descriptionEl = clone.querySelector('.weather-description');
                    const highLowEl = clone.querySelector('.weather-high-low');
                    const hourlyForecast = clone.querySelector('.hourly-forecast');

                    if (widget.weather) {
                        locationEl.textContent = widget.location;
                        currentTempEl.textContent = `${Math.round(widget.weather.current.temp)}°`;
                        descriptionEl.textContent = widget.weather.current.description || 'Clear';
                        highLowEl.textContent = `H:${Math.round(widget.weather.daily.high)}° L:${Math.round(widget.weather.daily.low)}°`;

                        // Render hourly forecast
                        widget.weather.hourly.forEach(hour => {
                            const hourClone = weatherHourlyTemplate.content.cloneNode(true);
                            hourClone.querySelector('.hourly-time').textContent = hour.time;
                            hourClone.querySelector('.hourly-temp').textContent = `${Math.round(hour.temp)}°`;

                            // Weather icon based on weather code
                            const iconEl = hourClone.querySelector('.hourly-icon');
                            const code = hour.weathercode;

                            // WMO Weather interpretation codes
                            if (code === 0) {
                                // Clear sky
                                iconEl.innerHTML = `
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <circle cx="12" cy="12" r="5"></circle>
                                    <line x1="12" y1="1" x2="12" y2="3"></line>
                                    <line x1="12" y1="21" x2="12" y2="23"></line>
                                    <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line>
                                    <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line>
                                    <line x1="1" y1="12" x2="3" y2="12"></line>
                                    <line x1="21" y1="12" x2="23" y2="12"></line>
                                    <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line>
                                    <line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line>
                                </svg>
                            `;
                                iconEl.setAttribute('title', 'Clear');
                            } else if (code >= 1 && code <= 3) {
                                // Partly cloudy / Cloudy
                                iconEl.innerHTML = `
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z"></path>
                                </svg>
                            `;
                                iconEl.setAttribute('title', code === 3 ? 'Overcast' : 'Partly Cloudy');
                            } else if (code >= 45 && code <= 48) {
                                // Fog
                                iconEl.innerHTML = `
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M9 19h6"></path>
                                    <path d="M9 15h10"></path>
                                    <path d="M5 11h14"></path>
                                </svg>
                            `;
                                iconEl.setAttribute('title', 'Foggy');
                            } else if ((code >= 51 && code <= 67) || (code >= 80 && code <= 82)) {
                                // Rain
                                iconEl.innerHTML = `
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <line x1="8" y1="19" x2="8" y2="21"></line>
                                    <line x1="8" y1="13" x2="8" y2="15"></line>
                                    <line x1="16" y1="19" x2="16" y2="21"></line>
                                    <line x1="16" y1="13" x2="16" y2="15"></line>
                                    <line x1="12" y1="21" x2="12" y2="23"></line>
                                    <line x1="12" y1="15" x2="12" y2="17"></line>
                                    <path d="M20 16.58A5 5 0 0 0 18 7h-1.26A8 8 0 1 0 4 15.25"></path>
                                </svg>
                            `;
                                iconEl.setAttribute('title', `Rain ${hour.precipitation > 0 ? `(${hour.precipitation}mm)` : ''}`);
                            } else if ((code >= 71 && code <= 77) || (code >= 85 && code <= 86)) {
                                // Snow
                                iconEl.innerHTML = `
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M20 17.58A5 5 0 0 0 18 8h-1.26A8 8 0 1 0 4 16.25"></path>
                                    <line x1="8" y1="16" x2="8" y2="16"></line>
                                    <line x1="8" y1="20" x2="8" y2="20"></line>
                                    <line x1="12" y1="18" x2="12" y2="18"></line>
                                    <line x1="12" y1="22" x2="12" y2="22"></line>
                                    <line x1="16" y1="16" x2="16" y2="16"></line>
                                    <line x1="16" y1="20" x2="16" y2="20"></line>
                                </svg>
                            `;
                                iconEl.setAttribute('title', 'Snow');
                            } else if (code >= 95) {
                                // Thunderstorm (95, 96, 99)
                                iconEl.innerHTML = `
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M19 16.9A5 5 0 0 0 18 7h-1.26a8 8 0 1 0-11.62 9"></path>
                                    <polyline points="13 11 9 17 15 17 11 23"></polyline>
                                </svg>
                            `;
                                iconEl.setAttribute('title', 'Thunderstorm');
                            } else {
                                // Default to cloudy for any unhandled codes
                                iconEl.innerHTML = `
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z"></path>
                                </svg>
                            `;
                                iconEl.setAttribute('title', 'Cloudy');
                            }

                            hourlyForecast.appendChild(hourClone);
                        });
                    } else {
                        locationEl.textContent = widget.location || 'Loading...';
                        currentTempEl.textContent = '--°';
                        descriptionEl.textContent = 'Loading...';
                        highLowEl.textContent = 'H:-- L:--';
                    }

                    // Edit location button
                    const editLocationBtn = clone.querySelector('.edit-location-btn');
                    editLocationBtn.onclick = () => editWeatherLocation(widget.id);

                    // Edit title button
                    const editTitleBtn = clone.querySelector('.edit-title-btn');
                    editTitleBtn.onclick = () => editWidgetTitle(widget.id);
                } else if (widget.type === 'todo') {
                    clone = todoTemplate.content.cloneNode(true);
                    widgetEl = clone.querySelector('.widget');
                    widgetEl.dataset.widgetId = widget.id;

                    clone.querySelector('.widget-title').textContent = widget.title;

                    const todoList = clone.querySelector('.todo-list');
                    if (widget.todos && widget.todos.length > 0) {
                        widget.todos.forEach(todo => {
                            const todoClone = todoItemTemplate.content.cloneNode(true);

                            const checkbox = todoClone.querySelector('.todo-checkbox');
                            checkbox.checked = todo.completed;
                            checkbox.onchange = () => toggleTodo(widget.id, todo.id);

                            const text = todoClone.querySelector('.todo-text');
                            text.textContent = todo.text;
                            if (todo.completed) {
                                text.style.textDecoration = 'line-through';
                                text.style.opacity = '0.6';
                            }

                            const deleteBtn = todoClone.querySelector('.todo-delete-btn');
                            deleteBtn.onclick = () => deleteTodo(widget.id, todo.id);

                            todoList.appendChild(todoClone);
                        });
                    } else {
                        todoList.innerHTML = '<div class="empty-todos">No tasks yet</div>';
                    }

                    const addTodoBtn = clone.querySelector('.add-todo-btn');
                    addTodoBtn.onclick = () => addTodo(widget.id);

                    const editTitleBtn3 = clone.querySelector('.edit-title-btn');
                    editTitleBtn3.onclick = () => editWidgetTitle(widget.id);
                }

                // Delete widget button
                const deleteBtn = clone.querySelector('.delete-widget-btn');
                deleteBtn.onclick = () => deleteWidget(widget.id);

                // Drag and drop
                if (widgetEl) {
                    widgetEl.addEventListener('dragstart', handleDragStart);
                    widgetEl.addEventListener('dragend', handleDragEnd);
                    widgetEl.addEventListener('dragover', handleDragOver);
                    widgetEl.addEventListener('drop', handleDrop);
                }

                widgetsGrid.appendChild(clone);
            } catch (error) {
                console.error('Error rendering widget:', widget.type, error);
            }
        });

        // Add the "Add Widget" button at the end
        const addWidgetCard = document.createElement('div');
        addWidgetCard.className = 'add-widget-card';
        addWidgetCard.innerHTML = `
            <button class="add-widget-btn-inline">
                <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <line x1="12" y1="5" x2="12" y2="19"></line>
                    <line x1="5" y1="12" x2="19" y2="12"></line>
                </svg>
                <span>Add Widget</span>
            </button>
        `;
        const addBtn = addWidgetCard.querySelector('.add-widget-btn-inline');
        if (addBtn) {
            addBtn.onclick = openWidgetModal;
        }
        widgetsGrid.appendChild(addWidgetCard);
        console.log('Add Widget button rendered, widgets count:', state.widgets.length);

        // Auto-resize all notes textareas after DOM is updated
        requestAnimationFrame(() => {
            document.querySelectorAll('.notes-textarea').forEach(textarea => {
                autoResizeTextarea(textarea);
            });
        });
    }

    function autoResizeTextarea(textarea) {
        textarea.style.height = 'auto';
        textarea.style.height = textarea.scrollHeight + 'px';
    }

    function getFaviconUrl(url) {
        try {
            const urlObj = new URL(url);
            // Use Google's favicon service as a reliable fallback
            return `https://www.google.com/s2/favicons?domain=${urlObj.hostname}&sz=32`;
        } catch (e) {
            // Return a default icon if URL is invalid
            return 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path></svg>';
        }
    }

    function getLargeFaviconUrl(url) {
        try {
            const urlObj = new URL(url);
            // Use Google's favicon service with larger size for launchpad
            return `https://www.google.com/s2/favicons?domain=${urlObj.hostname}&sz=64`;
        } catch (e) {
            // Return a default icon if URL is invalid
            return 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path></svg>';
        }
    }

    function editWidgetTitle(widgetId) {
        const widget = state.widgets.find(w => w.id === widgetId);
        if (!widget) return;

        const newTitle = prompt('Enter new widget title:', widget.title || '');
        if (newTitle && newTitle.trim()) {
            widget.title = newTitle.trim();
            saveWidgets();
            renderWidgets();
        }
    }

    function editWeatherLocation(widgetId) {
        const widget = state.widgets.find(w => w.id === widgetId);
        if (!widget) return;

        const newLocation = prompt('Enter new location:', widget.location || '');
        if (newLocation && newLocation.trim()) {
            widget.location = newLocation.trim();
            widget.weather = null; // Reset weather data
            saveWidgets();
            renderWidgets();
            fetchWeather(widgetId, newLocation.trim());
        }
    }

    // Drag and drop handlers
    function handleDragStart(e) {
        state.draggedWidget = e.currentTarget.dataset.widgetId;
        e.currentTarget.style.opacity = '0.5';
    }

    function handleDragEnd(e) {
        e.currentTarget.style.opacity = '1';
        state.draggedWidget = null;
    }

    function handleDragOver(e) {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
    }

    function handleDrop(e) {
        e.preventDefault();
        const targetWidgetId = e.currentTarget.dataset.widgetId;
        const draggedWidgetId = state.draggedWidget;

        if (draggedWidgetId && targetWidgetId && draggedWidgetId !== targetWidgetId) {
            const draggedIndex = state.widgets.findIndex(w => w.id === draggedWidgetId);
            const targetIndex = state.widgets.findIndex(w => w.id === targetWidgetId);

            // Swap positions
            const temp = state.widgets[draggedIndex];
            state.widgets[draggedIndex] = state.widgets[targetIndex];
            state.widgets[targetIndex] = temp;

            saveWidgets();
            renderWidgets();
        }
    }

    // Weather API (using Open-Meteo - free, no API key needed)
    async function fetchWeather(widgetId, location) {
        try {
            // First, geocode the location
            const geoRes = await fetch(`https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(location)}&count=1`);
            const geoData = await geoRes.json();

            if (!geoData.results || geoData.results.length === 0) {
                console.error('Location not found');
                return;
            }

            const { latitude, longitude } = geoData.results[0];

            // Fetch weather with hourly and daily data
            const weatherRes = await fetch(
                `https://api.open-meteo.com/v1/forecast?` +
                `latitude=${latitude}&longitude=${longitude}&` +
                `current_weather=true&` +
                `hourly=temperature_2m,precipitation,weathercode&` +
                `daily=temperature_2m_max,temperature_2m_min&` +
                `timezone=auto`
            );
            const weatherData = await weatherRes.json();

            // Get current hour index
            const now = new Date();
            const currentHour = now.getHours();

            // Get next 4 hours of data
            const hourlyData = [];
            for (let i = 0; i < 4; i++) {
                const hourIndex = currentHour + i;
                if (hourIndex < weatherData.hourly.temperature_2m.length) {
                    const hourTime = new Date(weatherData.hourly.time[hourIndex]);
                    hourlyData.push({
                        time: hourTime.getHours() + ':00',
                        temp: weatherData.hourly.temperature_2m[hourIndex],
                        precipitation: weatherData.hourly.precipitation[hourIndex] || 0,
                        weathercode: weatherData.hourly.weathercode[hourIndex]
                    });
                }
            }

            // Get weather description based on current conditions
            const currentWeather = weatherData.current_weather;
            let description = 'Clear';
            if (currentWeather.weathercode) {
                // WMO Weather interpretation codes
                const code = currentWeather.weathercode;
                if (code === 0) description = 'Clear';
                else if (code <= 3) description = 'Partly Cloudy';
                else if (code <= 48) description = 'Foggy';
                else if (code <= 67) description = 'Rainy';
                else if (code <= 77) description = 'Snowy';
                else if (code <= 82) description = 'Rainy';
                else if (code <= 86) description = 'Snowy';
                else description = 'Stormy';
            }

            // Update widget
            const widget = state.widgets.find(w => w.id === widgetId);
            if (widget) {
                widget.weather = {
                    current: {
                        temp: weatherData.current_weather.temperature,
                        description: description
                    },
                    daily: {
                        high: weatherData.daily.temperature_2m_max[0],
                        low: weatherData.daily.temperature_2m_min[0]
                    },
                    hourly: hourlyData
                };
                saveWidgets();
                renderWidgets();
            }
        } catch (error) {
            console.error('Error fetching weather:', error);
        }
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

    // --- Config Export/Import ---

    function objectToYaml(obj, indent = 0) {
        const spaces = '  '.repeat(indent);
        let yaml = '';

        for (const [key, value] of Object.entries(obj)) {
            if (value === null || value === undefined) {
                yaml += `${spaces}${key}: null\n`;
            } else if (Array.isArray(value)) {
                if (value.length === 0) {
                    yaml += `${spaces}${key}: []\n`;
                } else if (typeof value[0] === 'object') {
                    yaml += `${spaces}${key}:\n`;
                    value.forEach(item => {
                        yaml += `${spaces}- `;
                        const itemYaml = objectToYaml(item, indent + 1);
                        const lines = itemYaml.split('\n').filter(l => l.trim());
                        if (lines.length > 0) {
                            // First property on same line as dash
                            const firstLine = lines[0].trim();
                            yaml += `${firstLine}\n`;
                            // Rest indented
                            lines.slice(1).forEach(line => {
                                yaml += `${spaces}  ${line.trim()}\n`;
                            });
                        }
                    });
                } else {
                    yaml += `${spaces}${key}:\n`;
                    value.forEach(item => {
                        yaml += `${spaces}- ${item}\n`;
                    });
                }
            } else if (typeof value === 'object') {
                yaml += `${spaces}${key}:\n`;
                yaml += objectToYaml(value, indent + 1);
            } else if (typeof value === 'string') {
                // Escape special characters and use quotes if needed
                const needsQuotes = value.includes(':') || value.includes('#') || value.includes('\n');
                yaml += `${spaces}${key}: ${needsQuotes ? `"${value.replace(/"/g, '\\"')}"` : value}\n`;
            } else {
                yaml += `${spaces}${key}: ${value}\n`;
            }
        }

        return yaml;
    }

    function yamlToObject(yaml) {
        // Simple YAML parser for our config format
        const lines = yaml.split('\n');
        const result = {};
        const stack = [{ container: result, indent: -1 }];

        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            if (!line.trim() || line.trim().startsWith('#')) continue;

            const indent = line.search(/\S/);
            const trimmed = line.trim();

            // Pop stack to correct level
            while (stack.length > 1 && indent <= stack[stack.length - 1].indent) {
                stack.pop();
            }

            const parent = stack[stack.length - 1].container;

            // Handle array item
            if (trimmed.startsWith('-')) {
                const afterDash = trimmed.startsWith('- ') ? trimmed.substring(2) : trimmed.substring(1).trim();

                // Ensure parent is an array
                if (!Array.isArray(parent)) {
                    console.error('Parent is not an array for item:', trimmed);
                    continue;
                }

                // Check if it's an inline key-value or object
                if (afterDash.includes(':')) {
                    const colonIdx = afterDash.indexOf(':');
                    const key = afterDash.substring(0, colonIdx).trim();
                    const value = afterDash.substring(colonIdx + 1).trim();

                    const obj = {};
                    obj[key] = parseValue(value);
                    parent.push(obj);
                    stack.push({ container: obj, indent: indent });
                } else if (afterDash === '') {
                    // Empty dash means object follows
                    const obj = {};
                    parent.push(obj);
                    stack.push({ container: obj, indent: indent });
                } else {
                    // Simple value
                    parent.push(parseValue(afterDash));
                }
            }
            // Handle key-value pair
            else if (trimmed.includes(':')) {
                const colonIdx = trimmed.indexOf(':');
                const key = trimmed.substring(0, colonIdx).trim();
                const value = trimmed.substring(colonIdx + 1).trim();

                if (value === '' || value === '[]') {
                    // Look ahead to determine if it's array or object
                    const nextLine = i + 1 < lines.length ? lines[i + 1] : '';
                    const nextIndent = nextLine.search(/\S/);
                    const nextTrimmed = nextLine.trim();

                    if (value === '[]' || (nextTrimmed.startsWith('-') && nextIndent > indent)) {
                        // It's an array
                        parent[key] = [];
                        stack.push({ container: parent[key], indent: indent });
                    } else if (nextIndent > indent) {
                        // It's an object
                        parent[key] = {};
                        stack.push({ container: parent[key], indent: indent });
                    } else {
                        parent[key] = null;
                    }
                } else {
                    // Direct value
                    parent[key] = parseValue(value);
                }
            }
        }

        return result;

        function parseValue(val) {
            if (val === 'null' || val === '') return null;
            if (val === 'true') return true;
            if (val === 'false') return false;
            if (!isNaN(val) && val !== '') return Number(val);
            // Remove quotes
            return val.replace(/^["']|["']$/g, '').replace(/\\"/g, '"');
        }
    }

    function exportConfig() {
        // Clone widgets but remove weather data (keep only location)
        const widgetsToExport = state.widgets.map(widget => {
            if (widget.type === 'weather') {
                const { weather, ...widgetWithoutWeather } = widget;
                return widgetWithoutWeather;
            }
            return widget;
        });

        const config = {
            version: '1.0',
            exported: new Date().toISOString(),
            widgets: widgetsToExport,
            settings: state.userSettings
        };

        const yaml = objectToYaml(config);
        const blob = new Blob([yaml], { type: 'text/yaml' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `strt-config-${new Date().toISOString().split('T')[0]}.yaml`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    function importConfig(yamlText) {
        try {
            const config = yamlToObject(yamlText);
            console.log('Parsed YAML config:', config);
            console.log('Widgets array:', config.widgets);

            if (!config.widgets) {
                alert('Invalid config file: missing widgets data');
                return;
            }

            if (!Array.isArray(config.widgets)) {
                alert('Invalid config file: widgets must be an array');
                return;
            }

            console.log('Number of widgets to import:', config.widgets.length);

            if (confirm(`This will replace your current configuration with ${config.widgets.length} widget(s). Continue?`)) {
                state.widgets = config.widgets;
                if (config.settings) {
                    state.userSettings = { ...state.userSettings, ...config.settings };
                    applyTheme();
                }
                saveWidgets();
                saveSettings();
                renderWidgets();

                // Fetch weather data for any weather widgets
                state.widgets.forEach(widget => {
                    if (widget.type === 'weather' && widget.location) {
                        fetchWeather(widget.id, widget.location);
                    }
                });

                alert(`Configuration imported successfully! Loaded ${config.widgets.length} widget(s).`);
            }
        } catch (error) {
            console.error('Error importing config:', error);
            alert('Error importing configuration. Please check the file format.');
        }
    }

    function copyConfigToClipboard() {
        // Clone widgets but remove weather data (keep only location)
        const widgetsToExport = state.widgets.map(widget => {
            if (widget.type === 'weather') {
                const { weather, ...widgetWithoutWeather } = widget;
                return widgetWithoutWeather;
            }
            return widget;
        });

        const config = {
            version: '1.0',
            widgets: widgetsToExport,
            settings: state.userSettings
        };

        const yaml = objectToYaml(config);
        navigator.clipboard.writeText(yaml).then(() => {
            alert('Configuration copied to clipboard!');
        }).catch(err => {
            console.error('Error copying to clipboard:', err);
            alert('Error copying to clipboard. Please try export to file instead.');
        });
    }

    function pasteConfigFromClipboard() {
        navigator.clipboard.readText().then(text => {
            importConfig(text);
        }).catch(err => {
            console.error('Error reading from clipboard:', err);
            alert('Error reading from clipboard. Please try import from file instead.');
        });
    }

    // Setup export/import listeners
    const configManagerBtn = document.getElementById('config-manager-btn');
    const exportConfigAction = document.getElementById('export-config-action');
    const copyConfigAction = document.getElementById('copy-config-action');
    const importConfigAction = document.getElementById('import-config-action');
    const pasteConfigAction = document.getElementById('paste-config-action');
    const importFileInput = document.getElementById('import-file-input');

    if (configManagerBtn) {
        configManagerBtn.addEventListener('click', () => {
            switchView('config');
        });
    }

    if (exportConfigAction) {
        exportConfigAction.addEventListener('click', exportConfig);
    }

    if (copyConfigAction) {
        copyConfigAction.addEventListener('click', copyConfigToClipboard);
    }

    if (importConfigAction) {
        importConfigAction.addEventListener('click', () => {
            importFileInput.click();
        });
    }

    if (pasteConfigAction) {
        pasteConfigAction.addEventListener('click', pasteConfigFromClipboard);
    }

    if (importFileInput) {
        importFileInput.addEventListener('change', (e) => {
            const file = e.target.files[0];
            if (file) {
                const reader = new FileReader();
                reader.onload = (event) => {
                    importConfig(event.target.result);
                };
                reader.readAsText(file);
            }
            // Reset input so same file can be selected again
            e.target.value = '';
        });
    }


});
