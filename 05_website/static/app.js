// =============================================================================
// Aktien-Tool JavaScript
// =============================================================================

// Übersetzungen für Länder
const countryTranslations = {
    'Austria': 'Österreich',
    'Germany': 'Deutschland',
    'Switzerland': 'Schweiz',
    'United States': 'USA',
    'United Kingdom': 'Großbritannien',
    'France': 'Frankreich',
    'Italy': 'Italien',
    'Spain': 'Spanien',
    'Netherlands': 'Niederlande',
    'Belgium': 'Belgien',
    'Sweden': 'Schweden',
    'Norway': 'Norwegen',
    'Denmark': 'Dänemark',
    'Finland': 'Finnland',
    'Poland': 'Polen',
    'Czech Republic': 'Tschechien',
    'Hungary': 'Ungarn',
    'Portugal': 'Portugal',
    'Ireland': 'Irland',
    'Luxembourg': 'Luxemburg',
    'Greece': 'Griechenland',
    'Japan': 'Japan',
    'China': 'China',
    'Hong Kong': 'Hongkong',
    'South Korea': 'Südkorea',
    'Taiwan': 'Taiwan',
    'India': 'Indien',
    'Australia': 'Australien',
    'Canada': 'Kanada',
    'Brazil': 'Brasilien',
    'Mexico': 'Mexiko',
    'Russia': 'Russland',
    'Singapore': 'Singapur'
};

// Übersetzungen für Branchen (Industries)
const industryTranslations = {
    'Software': 'Software',
    'Hardware': 'Hardware',
    'Semiconductors': 'Halbleiter',
    'Banks': 'Banken',
    'Insurance': 'Versicherungen',
    'Asset Management': 'Vermögensverwaltung',
    'Biotechnology': 'Biotechnologie',
    'Pharmaceuticals': 'Pharma',
    'Medical Devices': 'Medizintechnik',
    'Healthcare Services': 'Gesundheitsdienste',
    'Automobiles': 'Automobile',
    'Auto Parts': 'Autozulieferer',
    'Aerospace & Defense': 'Luft- & Raumfahrt',
    'Industrial Machinery': 'Maschinenbau',
    'Electrical Equipment': 'Elektrotechnik',
    'Chemicals': 'Chemie',
    'Construction Materials': 'Baustoffe',
    'Construction & Engineering': 'Bau & Ingenieurwesen',
    'Retail': 'Einzelhandel',
    'Food & Beverage': 'Lebensmittel & Getränke',
    'Household Products': 'Haushaltsprodukte',
    'Apparel': 'Bekleidung',
    'Luxury Goods': 'Luxusgüter',
    'Media': 'Medien',
    'Entertainment': 'Unterhaltung',
    'Telecommunications': 'Telekommunikation',
    'Internet': 'Internet',
    'Oil & Gas': 'Öl & Gas',
    'Utilities': 'Versorger',
    'Renewable Energy': 'Erneuerbare Energien',
    'Mining': 'Bergbau',
    'Steel': 'Stahl',
    'Real Estate': 'Immobilien',
    'REITs': 'Immobilienfonds',
    'Transportation': 'Transport',
    'Airlines': 'Fluggesellschaften',
    'Shipping': 'Schifffahrt',
    'Logistics': 'Logistik',
    'Hotels & Restaurants': 'Hotels & Gastronomie',
    'Travel & Leisure': 'Reisen & Freizeit',
    'Education': 'Bildung',
    'Professional Services': 'Unternehmensberatung',
    'Diversified': 'Diversifiziert',
    'Conglomerates': 'Mischkonzerne'
};

function translateValue(value, field) {
    if (field === 'country' && countryTranslations[value]) {
        return countryTranslations[value];
    }
    if (field === 'industry' && industryTranslations[value]) {
        return industryTranslations[value];
    }
    return value;
}

// View-Name aus URL ermitteln
function getCurrentView() {
    const path = window.location.pathname;
    if (path.includes('watchlist')) return 'watchlist';
    if (path.includes('screener')) return 'screener';
    return 'watchlist';
}

document.addEventListener('DOMContentLoaded', function() {

    // =========================================================================
    // Unified Modal State (alle 8 Tabs in einem Modal)
    // =========================================================================
    let modalState = {
        isin: null,
        activeTab: 'pe',
        cache: {
            detailData: null,   // /api/stock/{isin}/details → KGV, EV/EBIT, Wachstum, Margen
            infoData: null,     // /api/stock/{isin}/info → Info
            priceData: null,    // /api/stock/{isin}/price-history → Chart
            earningsData: null, // /api/stock/{isin}/earnings → Earnings
            dcfData: null       // /api/stock/{isin}/dcf-data → DCF
        }
    };
    let peChart = null;
    let incomeChart = null;
    let evEbitChart = null;
    let ebitChart = null;
    let growthRevenueChart = null;
    let growthEbitChart = null;
    let growthNetIncomeChart = null;
    let marginsChart = null;
    let fullscreenChart = null;

    // Daten für Chart-Vollbild-Ansicht
    let chartData = {
        pe: null,
        evEbit: null
    };

    // =========================================================================
    // Tabellensortierung
    // =========================================================================
    let currentSortColumn = null;
    let currentSortDirection = 'asc';

    function initTableSorting() {
        const table = document.querySelector('.stock-table');
        if (!table) return;

        const headers = table.querySelectorAll('th.sortable');
        const tbody = table.querySelector('tbody');
        if (!tbody) return;

        headers.forEach(header => {
            header.addEventListener('click', function() {
                const column = this.dataset.column;
                const type = this.dataset.type || 'text';

                // Sortierrichtung bestimmen
                if (currentSortColumn === column) {
                    currentSortDirection = currentSortDirection === 'asc' ? 'desc' : 'asc';
                } else {
                    currentSortColumn = column;
                    currentSortDirection = 'asc';
                }

                // Visuelle Indikatoren aktualisieren
                headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
                this.classList.add(currentSortDirection === 'asc' ? 'sort-asc' : 'sort-desc');

                // Zeilen sortieren
                const rows = Array.from(tbody.querySelectorAll('tr'));
                rows.sort((a, b) => {
                    const cellA = a.querySelector(`td[data-column="${column}"]`);
                    const cellB = b.querySelector(`td[data-column="${column}"]`);

                    let valA = cellA?.dataset.value ?? '';
                    let valB = cellB?.dataset.value ?? '';

                    // Leere Werte ans Ende
                    if (valA === '' && valB !== '') return 1;
                    if (valA !== '' && valB === '') return -1;
                    if (valA === '' && valB === '') return 0;

                    let comparison = 0;
                    if (type === 'number') {
                        const numA = parseFloat(valA);
                        const numB = parseFloat(valB);
                        comparison = numA - numB;
                    } else if (type === 'date') {
                        // Datumsvergleich (Format: YYYY-MM-DD)
                        // ISO-Format sortiert auch alphabetisch korrekt
                        comparison = valA.localeCompare(valB);
                    } else {
                        comparison = valA.localeCompare(valB, 'de');
                    }

                    return currentSortDirection === 'asc' ? comparison : -comparison;
                });

                // Sortierte Zeilen wieder einfügen
                rows.forEach(row => tbody.appendChild(row));
            });
        });
    }

    initTableSorting();

    // =========================================================================
    // Favoriten ändern
    // =========================================================================
    document.querySelectorAll('.favorite-select').forEach(select => {
        select.addEventListener('change', async function() {
            const isin = this.dataset.isin;
            const favorite = parseInt(this.value);

            try {
                const response = await fetch('/api/favorite', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ isin, favorite })
                });

                if (response.ok) {
                    // Visuelles Feedback
                    this.style.background = '#d4edda';
                    setTimeout(() => {
                        this.style.background = '';
                    }, 500);
                }
            } catch (error) {
                console.error('Fehler:', error);
                alert('Fehler beim Speichern');
            }
        });
    });

    // =========================================================================
    // Notizen Modal
    // =========================================================================
    const noteModal = document.getElementById('note-modal');
    const noteText = document.getElementById('note-text');
    let currentNoteIsin = null;

    // Notiz-Button klicken
    document.querySelectorAll('.note-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            currentNoteIsin = this.dataset.isin;
            noteText.value = this.dataset.notes || '';
            noteModal.classList.remove('hidden');
            noteText.focus();
        });
    });

    // Notiz speichern
    const noteSaveBtn = document.getElementById('note-save');
    if (noteSaveBtn) {
        noteSaveBtn.addEventListener('click', async function() {
            if (!currentNoteIsin) return;

            try {
                const response = await fetch('/api/note', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        isin: currentNoteIsin,
                        notes: noteText.value
                    })
                });

                if (response.ok) {
                    // Button aktualisieren
                    const btn = document.querySelector(`.note-btn[data-isin="${currentNoteIsin}"]`);
                    if (btn) {
                        btn.dataset.notes = noteText.value;
                        btn.textContent = noteText.value ? '📝' : '+';
                    }
                    noteModal.classList.add('hidden');
                }
            } catch (error) {
                console.error('Fehler:', error);
                alert('Fehler beim Speichern');
            }
        });
    }

    // =========================================================================
    // Info Modal (Beschreibung)
    // =========================================================================
    const infoModal = document.getElementById('info-modal');
    const infoTitle = document.getElementById('info-title');
    const infoBody = document.getElementById('info-body');

    document.querySelectorAll('.info-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            infoTitle.textContent = this.dataset.name || 'Unternehmen';
            infoBody.innerHTML = `<p>${this.dataset.description || 'Keine Beschreibung vorhanden.'}</p>`;
            infoModal.classList.remove('hidden');
        });
    });

    // =========================================================================
    // Modal schließen
    // =========================================================================
    function resetModalState(modal) {
        if (!modal) return;

        if (modal.id === 'stock-detail-modal') {
            // Reset unified modal state
            modalState.isin = null;
            modalState.activeTab = 'pe';
            modalState.cache = {
                detailData: null,
                infoData: null,
                priceData: null,
                earningsData: null,
                dcfData: null
            };

            // Destroy all charts
            if (priceChart) { priceChart.destroy(); priceChart = null; }
            if (peChart) { peChart.destroy(); peChart = null; }
            if (incomeChart) { incomeChart.destroy(); incomeChart = null; }
            if (evEbitChart) { evEbitChart.destroy(); evEbitChart = null; }
            if (ebitChart) { ebitChart.destroy(); ebitChart = null; }
            if (growthRevenueChart) { growthRevenueChart.destroy(); growthRevenueChart = null; }
            if (growthEbitChart) { growthEbitChart.destroy(); growthEbitChart = null; }
            if (growthNetIncomeChart) { growthNetIncomeChart.destroy(); growthNetIncomeChart = null; }
            if (marginsChart) { marginsChart.destroy(); marginsChart = null; }
            if (dcfChart) { dcfChart.destroy(); dcfChart = null; }

            // Reset tabs visual
            const tabsEl = document.getElementById('unified-tabs');
            if (tabsEl) {
                tabsEl.querySelectorAll('.detail-tab').forEach(t => {
                    t.classList.toggle('active', t.dataset.tab === 'pe');
                });
            }
        }
    }

    document.querySelectorAll('.modal-close').forEach(btn => {
        btn.addEventListener('click', function() {
            const modal = this.closest('.modal');
            modal.classList.add('hidden');
            resetModalState(modal);
        });
    });

    // Klick außerhalb Modal
    document.querySelectorAll('.modal').forEach(modal => {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                this.classList.add('hidden');
                resetModalState(this);
            }
        });
    });

    // Escape-Taste
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal').forEach(modal => {
                modal.classList.add('hidden');
                resetModalState(modal);
            });
        }
    });

    // =========================================================================
    // Favoriten-Konfiguration Modal
    // =========================================================================
    const favoritesBtn = document.getElementById('favorites-btn');
    const favoritesModal = document.getElementById('favorites-modal');
    const labelList = document.getElementById('label-list');
    const filterList = document.getElementById('filter-list');
    const favoritesSaveBtn = document.getElementById('favorites-save');

    let favoritesData = { labels: {}, filters: {} };

    if (favoritesBtn) {
        favoritesBtn.addEventListener('click', async function() {
            favoritesModal.classList.remove('hidden');
            await loadFavoriteSettings();
        });
    }

    async function loadFavoriteSettings() {
        try {
            const response = await fetch('/api/favorite-settings');
            favoritesData = await response.json();
            renderFavoriteSettings();
        } catch (error) {
            console.error('Fehler:', error);
        }
    }

    function renderFavoriteSettings() {
        // Badge-Farben für die Nummern
        const badgeColors = {
            1: { bg: '#d4edda', color: '#155724' },
            2: { bg: '#fff3cd', color: '#856404' },
            3: { bg: '#cce5ff', color: '#004085' },
            4: { bg: '#f8d7da', color: '#721c24' },
            5: { bg: '#e2d5f1', color: '#4a235a' },
            6: { bg: '#d1ecf1', color: '#0c5460' },
            7: { bg: '#ffeeba', color: '#856404' },
            8: { bg: '#c3e6cb', color: '#155724' },
            9: { bg: '#d6d8db', color: '#383d41' }
        };

        // Labels rendern
        if (labelList) {
            let html = '';
            for (let i = 1; i <= 9; i++) {
                const label = favoritesData.labels[i] || `Favorit ${i}`;
                const colors = badgeColors[i];
                html += `
                    <div class="label-item">
                        <span class="label-number" style="background: ${colors.bg}; color: ${colors.color}">${i}</span>
                        <input type="text" data-id="${i}" value="${label}" placeholder="Name eingeben...">
                    </div>
                `;
            }
            labelList.innerHTML = html;
        }

        // Filter rendern
        if (filterList) {
            let html = '';
            for (let i = 1; i <= 9; i++) {
                const label = favoritesData.labels[i] || `Favorit ${i}`;
                const isVisible = favoritesData.filters[i] !== false;
                html += `
                    <label class="filter-item">
                        <input type="checkbox" data-id="${i}" ${isVisible ? 'checked' : ''}>
                        <span class="filter-number">${i}</span>
                        <span class="filter-label">${label}</span>
                    </label>
                `;
            }
            filterList.innerHTML = html;
        }

        // Label-Änderungen auf Filter-Liste übertragen
        if (labelList) {
            labelList.querySelectorAll('input[type="text"]').forEach(input => {
                input.addEventListener('input', function() {
                    const id = this.dataset.id;
                    const value = this.value;
                    // Filter-Label aktualisieren
                    const filterLabel = filterList.querySelector(`[data-id="${id}"]`);
                    if (filterLabel) {
                        filterLabel.closest('.filter-item').querySelector('.filter-label').textContent = value || `Favorit ${id}`;
                    }
                });
            });
        }
    }

    // Favoriten speichern
    if (favoritesSaveBtn) {
        favoritesSaveBtn.addEventListener('click', async function() {
            const labels = {};
            const filters = {};

            // Labels sammeln
            if (labelList) {
                labelList.querySelectorAll('input[type="text"]').forEach(input => {
                    labels[input.dataset.id] = input.value;
                });
            }

            // Filter sammeln
            if (filterList) {
                filterList.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
                    filters[checkbox.dataset.id] = checkbox.checked;
                });
            }

            try {
                const response = await fetch('/api/favorite-settings', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ labels, filters })
                });

                if (response.ok) {
                    window.location.reload();
                } else {
                    alert('Fehler beim Speichern');
                }
            } catch (error) {
                console.error('Fehler:', error);
                alert('Fehler beim Speichern');
            }
        });
    }

    // =========================================================================
    // Spalten-Konfiguration Modal
    // =========================================================================
    const settingsBtn = document.getElementById('settings-btn');
    const columnsModal = document.getElementById('columns-modal');
    const columnsBody = document.getElementById('columns-body');
    const columnsSaveBtn = document.getElementById('columns-save');

    let columnsData = [];

    if (settingsBtn) {
        settingsBtn.addEventListener('click', async function() {
            columnsModal.classList.remove('hidden');
            await loadColumns();
        });
    }

    async function loadColumns() {
        const viewName = getCurrentView();
        columnsBody.innerHTML = '<p class="loading">Lade Spalten...</p>';

        try {
            const response = await fetch(`/api/columns/${viewName}`);
            const data = await response.json();

            columnsData = data.columns;
            renderColumnsList(data.groups);

        } catch (error) {
            console.error('Fehler:', error);
            columnsBody.innerHTML = '<p class="error">Fehler beim Laden</p>';
        }
    }

    // Spalten-Daten global speichern für Zugriff in Event-Handlern
    let allColumns = [];

    function renderColumnsList(groups) {
        // Alle Spalten in flaches Array sammeln
        allColumns = [];
        const groupOrder = ['Stammdaten', 'Kursdaten', 'Bewertung', 'Durchschnitte', 'KGV Abweichung', 'EV/EBIT Abweichung', 'Wachstum', 'Bilanz', 'Margen'];
        for (const groupName of groupOrder) {
            if (groups[groupName]) {
                allColumns.push(...groups[groupName]);
            }
        }

        // Sichtbare Spalten nach sort_order sortieren
        const visibleColumns = allColumns
            .filter(col => col.is_visible)
            .sort((a, b) => a.sort_order - b.sort_order);

        let html = '<div class="columns-config-split">';

        // Linke Seite: Auswahl
        html += '<div class="columns-selection">';
        html += '<h4>Spalten auswählen</h4>';
        html += '<p class="hint">Checkboxen zum Ein-/Ausblenden</p>';

        for (const groupName of groupOrder) {
            if (!groups[groupName]) continue;

            html += `<div class="column-group">`;
            html += `<h5>${groupName}</h5>`;
            html += `<ul class="column-list" data-group="${groupName}">`;

            const sortedCols = groups[groupName].sort((a, b) => a.sort_order - b.sort_order);

            for (const col of sortedCols) {
                html += `
                    <li class="column-item" data-key="${col.column_key}" data-name="${col.display_name}">
                        <label>
                            <input type="checkbox" ${col.is_visible ? 'checked' : ''}>
                            ${col.display_name}
                        </label>
                    </li>
                `;
            }

            html += `</ul></div>`;
        }
        html += '</div>';

        // Rechte Seite: Reihenfolge
        html += '<div class="columns-order">';
        html += '<h4>Reihenfolge festlegen</h4>';
        html += '<p class="hint">Drag & Drop zum Sortieren</p>';
        html += '<ul class="order-list">';

        for (const col of visibleColumns) {
            html += `
                <li class="order-item" draggable="true" data-key="${col.column_key}">
                    <span class="drag-handle">☰</span>
                    <span class="order-name">${col.display_name}</span>
                </li>
            `;
        }

        html += '</ul></div>';
        html += '</div>';

        columnsBody.innerHTML = html;

        // Event-Listener für Checkboxen
        initCheckboxListeners();
        // Drag & Drop für Sortier-Liste
        initOrderDragAndDrop();
    }

    function initCheckboxListeners() {
        document.querySelectorAll('.column-item input[type="checkbox"]').forEach(checkbox => {
            checkbox.addEventListener('change', function() {
                const item = this.closest('.column-item');
                const key = item.dataset.key;
                const name = item.dataset.name;
                const orderList = document.querySelector('.order-list');

                if (this.checked) {
                    // Zur Sortier-Liste hinzufügen
                    const li = document.createElement('li');
                    li.className = 'order-item';
                    li.draggable = true;
                    li.dataset.key = key;
                    li.innerHTML = `
                        <span class="drag-handle">☰</span>
                        <span class="order-name">${name}</span>
                    `;
                    orderList.appendChild(li);
                    initOrderDragAndDrop();
                } else {
                    // Aus Sortier-Liste entfernen
                    const orderItem = orderList.querySelector(`[data-key="${key}"]`);
                    if (orderItem) {
                        orderItem.remove();
                    }
                }
            });
        });
    }

    function initOrderDragAndDrop() {
        const orderList = document.querySelector('.order-list');
        if (!orderList) return;

        const items = orderList.querySelectorAll('.order-item');

        items.forEach(item => {
            item.addEventListener('dragstart', handleOrderDragStart);
            item.addEventListener('dragend', handleOrderDragEnd);
            item.addEventListener('dragover', handleOrderDragOver);
            item.addEventListener('drop', handleOrderDrop);
        });
    }

    let draggedOrderItem = null;

    function handleOrderDragStart(e) {
        draggedOrderItem = this;
        this.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
    }

    function handleOrderDragEnd(e) {
        this.classList.remove('dragging');
        document.querySelectorAll('.order-item').forEach(item => {
            item.classList.remove('drag-over');
        });
    }

    function handleOrderDragOver(e) {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';

        const targetItem = this;
        if (targetItem !== draggedOrderItem) {
            targetItem.classList.add('drag-over');
        }
    }

    function handleOrderDrop(e) {
        e.preventDefault();
        const targetItem = this;

        if (targetItem !== draggedOrderItem) {
            const list = targetItem.parentNode;
            const items = Array.from(list.children);
            const draggedIndex = items.indexOf(draggedOrderItem);
            const targetIndex = items.indexOf(targetItem);

            if (draggedIndex < targetIndex) {
                targetItem.after(draggedOrderItem);
            } else {
                targetItem.before(draggedOrderItem);
            }
        }

        targetItem.classList.remove('drag-over');
    }

    // Spalten speichern
    if (columnsSaveBtn) {
        columnsSaveBtn.addEventListener('click', async function() {
            const viewName = getCurrentView();
            const updates = [];
            let sortOrder = 1;

            // 1. Sichtbare Spalten aus der Sortier-Liste (Reihenfolge zählt!)
            const orderList = document.querySelector('.order-list');
            const visibleKeys = new Set();

            if (orderList) {
                orderList.querySelectorAll('.order-item').forEach(item => {
                    const columnKey = item.dataset.key;
                    visibleKeys.add(columnKey);
                    updates.push({
                        column_key: columnKey,
                        is_visible: true,
                        sort_order: sortOrder++
                    });
                });
            }

            // 2. Nicht-sichtbare Spalten hinzufügen (Reihenfolge egal)
            document.querySelectorAll('.column-item').forEach(item => {
                const columnKey = item.dataset.key;
                if (!visibleKeys.has(columnKey)) {
                    updates.push({
                        column_key: columnKey,
                        is_visible: false,
                        sort_order: sortOrder++
                    });
                }
            });

            try {
                const response = await fetch(`/api/columns/${viewName}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ columns: updates })
                });

                if (response.ok) {
                    // Seite neu laden um Änderungen anzuzeigen
                    window.location.reload();
                } else {
                    alert('Fehler beim Speichern');
                }
            } catch (error) {
                console.error('Fehler:', error);
                alert('Fehler beim Speichern');
            }
        });
    }

    // =========================================================================
    // Suchfeld (nur auf Screener-Seite)
    // =========================================================================
    const searchInput = document.getElementById('search-input');
    const searchBtn = document.getElementById('search-btn');

    if (searchInput && searchBtn) {
        // Suchen-Button klicken
        searchBtn.addEventListener('click', performSearch);

        // Enter-Taste im Suchfeld
        searchInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                performSearch();
            }
        });
    }

    async function performSearch() {
        const searchTerm = searchInput.value.trim();
        await applyFilters({ search: searchTerm });
    }

    // =========================================================================
    // Filter Modal (nur auf Screener-Seite)
    // =========================================================================
    const filterBtn = document.getElementById('filter-btn');
    const filterModal = document.getElementById('filter-modal');
    const filterBody = document.getElementById('filter-body');
    const filterApplyBtn = document.getElementById('filter-apply');
    const filterResetBtn = document.getElementById('filter-reset');

    let filterOptions = null;
    let numericFilterCount = 0;
    let currentFilters = {}; // Aktuelle Filter speichern

    // Filter aus localStorage beim Seitenladen wiederherstellen
    if (filterBtn) {
        try {
            const savedFilters = localStorage.getItem('screenerFilters');
            if (savedFilters) {
                currentFilters = JSON.parse(savedFilters);

                // Suchfeld wiederherstellen falls vorhanden
                if (currentFilters.search && searchInput) {
                    searchInput.value = currentFilters.search;
                }

                // Filter automatisch anwenden
                applyFilters();
            }
        } catch (e) {
            console.error('Fehler beim Laden der gespeicherten Filter:', e);
        }
    }

    if (filterBtn && filterModal) {
        filterBtn.addEventListener('click', async function() {
            filterModal.classList.remove('hidden');
            await loadFilterOptions();
        });
    }

    async function loadFilterOptions() {
        if (filterOptions) {
            renderFilterForm();
            return;
        }

        filterBody.innerHTML = '<p class="loading">Lade Filter...</p>';

        try {
            const response = await fetch('/api/filter-options');
            filterOptions = await response.json();
            renderFilterForm();
        } catch (error) {
            console.error('Fehler:', error);
            filterBody.innerHTML = '<p class="error">Fehler beim Laden</p>';
        }
    }

    function renderFilterForm() {
        let html = '<div class="filter-form">';

        // Kategorische Filter
        html += '<div class="filter-section">';
        html += '<h4>Kategorien</h4>';
        html += '<div class="filter-grid">';

        const categoryLabels = {
            'stock_index': 'Index',
            'sector': 'Sektor',
            'industry': 'Branche',
            'country': 'Land'
        };

        for (const [field, values] of Object.entries(filterOptions.categorical)) {
            html += `
                <div class="filter-field">
                    <label>${categoryLabels[field] || field}</label>
                    <div class="filter-dropdown" id="filter-${field}" data-field="${field}">
                        <button type="button" class="filter-dropdown-toggle">
                            <span class="filter-dropdown-text">Alle</span>
                            <span class="filter-dropdown-arrow">▼</span>
                        </button>
                        <div class="filter-dropdown-menu">
                            ${values.map(v => `
                                <label class="filter-checkbox-item">
                                    <input type="checkbox" value="${v}" name="filter-${field}">
                                    <span>${translateValue(v, field)}</span>
                                </label>
                            `).join('')}
                        </div>
                    </div>
                </div>
            `;
        }

        html += '</div></div>';

        // Numerische Filter
        html += '<div class="filter-section">';
        html += '<h4>Kennzahlen</h4>';
        html += '<div id="numeric-filters"></div>';
        html += '<button type="button" id="add-numeric-filter" class="btn btn-small">+ Filter hinzufügen</button>';
        html += '</div>';

        html += '</div>';
        filterBody.innerHTML = html;

        // Event Listener für "Filter hinzufügen"
        numericFilterCount = 0;
        document.getElementById('add-numeric-filter').addEventListener('click', addNumericFilter);

        // Event Listener für Filter-Dropdowns
        initFilterDropdowns();

        // Gespeicherte Filter wiederherstellen
        restoreSavedFilters();
    }

    function initFilterDropdowns() {
        // Toggle-Buttons
        document.querySelectorAll('.filter-dropdown-toggle').forEach(toggle => {
            toggle.addEventListener('click', function(e) {
                e.stopPropagation();
                const dropdown = this.closest('.filter-dropdown');
                const wasOpen = dropdown.classList.contains('open');

                // Alle anderen schließen
                document.querySelectorAll('.filter-dropdown.open').forEach(d => d.classList.remove('open'));

                // Dieses öffnen/schließen
                if (!wasOpen) {
                    dropdown.classList.add('open');
                }
            });
        });

        // Checkboxen - Text aktualisieren bei Änderung
        document.querySelectorAll('.filter-dropdown input[type="checkbox"]').forEach(cb => {
            cb.addEventListener('change', function() {
                updateDropdownText(this.closest('.filter-dropdown'));
            });
        });

        // Klick außerhalb schließt Dropdown
        document.addEventListener('click', function(e) {
            if (!e.target.closest('.filter-dropdown')) {
                document.querySelectorAll('.filter-dropdown.open').forEach(d => d.classList.remove('open'));
            }
        });
    }

    function updateDropdownText(dropdown) {
        const checked = dropdown.querySelectorAll('input[type="checkbox"]:checked');
        const textEl = dropdown.querySelector('.filter-dropdown-text');
        const field = dropdown.dataset.field;

        if (checked.length === 0) {
            textEl.textContent = 'Alle';
        } else if (checked.length === 1) {
            textEl.textContent = translateValue(checked[0].value, field);
        } else {
            textEl.textContent = `${checked.length} ausgewählt`;
        }
    }

    function addNumericFilter() {
        const container = document.getElementById('numeric-filters');
        const id = numericFilterCount++;

        // Numerische Spalten nach Gruppen sortieren
        const groupedColumns = {};
        for (const col of filterOptions.numeric) {
            const group = col.column_group || 'Sonstige';
            if (!groupedColumns[group]) {
                groupedColumns[group] = [];
            }
            groupedColumns[group].push(col);
        }

        let optionsHtml = '<option value="">Kennzahl wählen...</option>';
        const groupOrder = ['Kursdaten', 'Bewertung', 'Durchschnitte', 'KGV Abweichung', 'EV/EBIT Abweichung', 'Wachstum', 'Bilanz', 'Margen'];

        for (const group of groupOrder) {
            if (groupedColumns[group]) {
                optionsHtml += `<optgroup label="${group}">`;
                for (const col of groupedColumns[group]) {
                    optionsHtml += `<option value="${col.column_key}">${col.display_name}</option>`;
                }
                optionsHtml += '</optgroup>';
            }
        }

        const filterHtml = `
            <div class="numeric-filter-row" data-id="${id}">
                <select class="filter-column">
                    ${optionsHtml}
                </select>
                <select class="filter-operator">
                    <option value="<">&lt;</option>
                    <option value="<=">&le;</option>
                    <option value="=">=</option>
                    <option value=">=">&ge;</option>
                    <option value=">">&gt;</option>
                </select>
                <input type="number" class="filter-value" step="any" placeholder="Wert">
                <button type="button" class="btn-remove-filter" title="Entfernen">&times;</button>
            </div>
        `;

        container.insertAdjacentHTML('beforeend', filterHtml);

        // Event Listener für Entfernen-Button
        container.querySelector(`[data-id="${id}"] .btn-remove-filter`).addEventListener('click', function() {
            this.closest('.numeric-filter-row').remove();
        });
    }

    function restoreSavedFilters() {
        // Kategorische Filter wiederherstellen
        for (const field of ['stock_index', 'sector', 'industry', 'country']) {
            const dropdown = document.getElementById(`filter-${field}`);
            if (dropdown && currentFilters[field]) {
                const values = Array.isArray(currentFilters[field]) ? currentFilters[field] : [currentFilters[field]];
                const checkboxes = dropdown.querySelectorAll('input[type="checkbox"]');
                checkboxes.forEach(cb => {
                    cb.checked = values.includes(cb.value);
                });
                updateDropdownText(dropdown);
            }
        }

        // Numerische Filter wiederherstellen
        const container = document.getElementById('numeric-filters');
        if (container && currentFilters.numeric && currentFilters.numeric.length > 0) {
            // Vorhandene Filter entfernen (der eine leere Filter wurde bereits hinzugefügt)
            container.innerHTML = '';
            numericFilterCount = 0;

            // Gespeicherte Filter wiederherstellen
            currentFilters.numeric.forEach(filter => {
                addNumericFilter();
                const lastRow = container.lastElementChild;
                if (lastRow) {
                    lastRow.querySelector('.filter-column').value = filter.column;
                    lastRow.querySelector('.filter-operator').value = filter.operator;
                    lastRow.querySelector('.filter-value').value = filter.value;
                }
            });
        } else {
            // Wenn keine gespeicherten numerischen Filter vorhanden sind, einen leeren hinzufügen
            addNumericFilter();
        }
    }

    // Gemeinsame Filter-Funktion
    async function applyFilters(additionalFilters = {}) {
        const filters = { ...currentFilters, ...additionalFilters };

        // Suchfeld-Wert immer aktuell halten
        if (searchInput) {
            filters.search = searchInput.value.trim();
        }

        // Kategorische Filter aus Modal sammeln (falls vorhanden)
        for (const field of ['stock_index', 'sector', 'industry', 'country']) {
            const container = document.getElementById(`filter-${field}`);
            if (container) {
                const checked = Array.from(container.querySelectorAll('input[type="checkbox"]:checked'))
                    .map(cb => cb.value);
                if (checked.length > 0) {
                    filters[field] = checked;
                } else {
                    delete filters[field];
                }
            }
        }

        // Numerische Filter sammeln (falls vorhanden)
        const numericRows = document.querySelectorAll('.numeric-filter-row');
        if (numericRows.length > 0) {
            filters.numeric = [];
            numericRows.forEach(row => {
                const column = row.querySelector('.filter-column').value;
                const operator = row.querySelector('.filter-operator').value;
                const value = row.querySelector('.filter-value').value;

                if (column && value !== '') {
                    filters.numeric.push({ column, operator, value: parseFloat(value) });
                }
            });
        }

        // Aktuelle Filter speichern
        currentFilters = filters;

        // Filter in localStorage speichern für Persistenz
        try {
            localStorage.setItem('screenerFilters', JSON.stringify(filters));
        } catch (e) {
            console.error('Fehler beim Speichern der Filter:', e);
        }

        // UI-Feedback
        if (searchBtn) {
            searchBtn.textContent = 'Lade...';
            searchBtn.disabled = true;
        }
        if (filterApplyBtn) {
            filterApplyBtn.textContent = 'Lade...';
            filterApplyBtn.disabled = true;
        }

        try {
            const response = await fetch('/api/screener/filter', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filters })
            });

            const data = await response.json();
            updateScreenerTable(data);

            if (filterModal) {
                filterModal.classList.add('hidden');
            }

        } catch (error) {
            console.error('Fehler:', error);
            alert('Fehler beim Filtern');
        } finally {
            if (searchBtn) {
                searchBtn.textContent = 'Suchen';
                searchBtn.disabled = false;
            }
            if (filterApplyBtn) {
                filterApplyBtn.textContent = 'Anwenden';
                filterApplyBtn.disabled = false;
            }
        }
    }

    // Filter anwenden (Button im Modal)
    if (filterApplyBtn) {
        filterApplyBtn.addEventListener('click', function() {
            applyFilters();
        });
    }

    // Filter zurücksetzen
    if (filterResetBtn) {
        filterResetBtn.addEventListener('click', function() {
            // Suchfeld leeren
            if (searchInput) {
                searchInput.value = '';
            }

            // Kategorische Filter zurücksetzen
            for (const field of ['stock_index', 'sector', 'industry', 'country']) {
                const container = document.getElementById(`filter-${field}`);
                if (container) {
                    container.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
                }
            }

            // Numerische Filter entfernen
            const container = document.getElementById('numeric-filters');
            if (container) {
                container.innerHTML = '';
                numericFilterCount = 0;
                addNumericFilter();
            }

            // Gespeicherte Filter leeren
            currentFilters = {};

            // Auch aus localStorage entfernen
            try {
                localStorage.removeItem('screenerFilters');
            } catch (e) {
                console.error('Fehler beim Löschen der Filter:', e);
            }

            // Ungefilterte Daten laden
            applyFilters();
        });
    }

    // Tabelle mit gefilterten Daten aktualisieren
    function updateScreenerTable(data) {
        const resultInfo = document.getElementById('result-info');
        const tableContainer = document.getElementById('table-container');

        if (!tableContainer) return;

        // Info aktualisieren
        if (resultInfo) {
            resultInfo.textContent = `${data.count} Aktien gefunden.`;
        }

        if (data.stocks.length === 0) {
            tableContainer.innerHTML = '<p class="empty-state">Keine Aktien gefunden.</p>';
            return;
        }

        // Tabelle neu aufbauen
        let html = '<table class="stock-table" id="screener-table"><thead><tr>';
        html += '<th class="sortable" data-column="favorite" data-type="number">Fav</th>';

        for (const col of data.columns) {
            const numClass = col.format_type !== 'text' ? 'num' : '';
            // Korrekter data-type für Sortierung
            let dataType = 'text';
            if (col.format_type === 'date') {
                dataType = 'date';
            } else if (col.format_type !== 'text') {
                dataType = 'number';
            }
            html += `<th class="sortable ${numClass}" data-column="${col.column_key}" data-type="${dataType}">${col.display_name}</th>`;
        }
        html += '<th>Notizen</th></tr></thead><tbody>';

        for (const stock of data.stocks) {
            html += `<tr data-isin="${stock.isin}">`;
            let favOptions = `<option value="0" ${(!stock.favorite || stock.favorite == 0) ? 'selected' : ''}>-</option>`;
            for (let i = 1; i <= 9; i++) {
                favOptions += `<option value="${i}" ${stock.favorite == i ? 'selected' : ''}>${i}</option>`;
            }
            html += `<td data-column="favorite" data-label="Favorit" data-value="${stock.favorite || 0}">
                <select class="favorite-select" data-isin="${stock.isin}">
                    ${favOptions}
                </select>
            </td>`;

            for (const col of data.columns) {
                const value = stock[col.column_key];
                const numClass = col.format_type !== 'text' ? 'num' : '';
                const nameClass = col.column_key === 'company_name' ? 'name' : '';

                let displayValue = '-';
                if (value !== null && value !== undefined) {
                    if (col.format_type === 'percent') {
                        displayValue = parseFloat(value).toLocaleString('de-DE', {minimumFractionDigits: 1, maximumFractionDigits: 1}) + '%';
                    } else if (col.format_type === 'billions') {
                        displayValue = (value / 1000000000).toLocaleString('de-DE', {minimumFractionDigits: 1, maximumFractionDigits: 1});
                    } else if (col.format_type === 'currency') {
                        displayValue = parseFloat(value).toLocaleString('de-DE', {minimumFractionDigits: 2, maximumFractionDigits: 2});
                    } else if (col.format_type === 'number') {
                        displayValue = parseFloat(value).toLocaleString('de-DE', {minimumFractionDigits: 1, maximumFractionDigits: 1});
                    } else if (col.format_type === 'date') {
                        // Datum im deutschen Format anzeigen (DD.MM.YYYY)
                        const date = new Date(value);
                        if (!isNaN(date)) {
                            displayValue = date.toLocaleDateString('de-DE');
                        } else {
                            displayValue = value;
                        }
                    } else {
                        displayValue = value;
                    }
                }

                html += `<td class="${numClass} ${nameClass}" data-column="${col.column_key}" data-label="${col.display_name}" data-value="${value !== null && value !== undefined ? value : ''}">${displayValue}</td>`;
            }

            html += `<td class="notes-cell" data-label="Notizen">
                <button class="note-btn" data-isin="${stock.isin}" data-notes="${stock.notes || ''}">
                    ${stock.notes ? '📝' : '+'}
                </button>
            </td>`;
            html += '</tr>';
        }

        html += '</tbody></table>';
        tableContainer.innerHTML = html;

        // Event Listener für neue Elemente hinzufügen
        reinitializeEventListeners();
    }

    // Event Listener für dynamisch erstellte Elemente neu initialisieren
    function reinitializeEventListeners() {
        // Favoriten
        document.querySelectorAll('.favorite-select').forEach(select => {
            select.removeEventListener('change', handleFavoriteChange);
            select.addEventListener('change', handleFavoriteChange);
        });

        // Notizen
        document.querySelectorAll('.note-btn').forEach(btn => {
            btn.removeEventListener('click', handleNoteClick);
            btn.addEventListener('click', handleNoteClick);
        });

        // Clickable cells für Modals
        initializeClickableCells();

        // Company name clicks
        initializeCompanyNameClicks();

        // Tabellensortierung
        initTableSorting();
    }

    async function handleFavoriteChange() {
        const isin = this.dataset.isin;
        const favorite = parseInt(this.value);
        const select = this;

        try {
            const response = await fetch('/api/favorite', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ isin, favorite })
            });

            if (response.ok) {
                select.style.background = '#d4edda';
                setTimeout(() => {
                    select.style.background = '';
                }, 500);
            }
        } catch (error) {
            console.error('Fehler:', error);
            alert('Fehler beim Speichern');
        }
    }

    function handleNoteClick() {
        currentNoteIsin = this.dataset.isin;
        noteText.value = this.dataset.notes || '';
        noteModal.classList.remove('hidden');
        noteText.focus();
    }

    // =========================================================================
    // Stock Detail Modal (KGV + EV/EBIT mit Tabs)
    // =========================================================================
    const stockDetailModal = document.getElementById('stock-detail-modal');
    const detailCompanyName = document.getElementById('detail-company-name');
    const detailMeta = document.getElementById('detail-meta');
    const detailBody = document.getElementById('detail-body');
    // Unified Tab-Klicks mit Event Delegation am Modal-Container
    if (stockDetailModal) {
        stockDetailModal.addEventListener('click', function(e) {
            const tab = e.target.closest('.detail-tab');
            if (!tab) return;

            e.preventDefault();
            e.stopPropagation();

            const tabType = tab.dataset.tab;
            if (!modalState.isin) return;

            // Aktiven Tab wechseln
            stockDetailModal.querySelectorAll('.detail-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            modalState.activeTab = tabType;

            // Content laden via Dispatcher
            loadTabContent(tabType);
        });
    }

    // KGV-relevante Spalten, die das PE-Modal öffnen (Set für O(1) Lookup)
    const PE_COLUMNS = new Set([
        'ttm_pe', 'fy_pe',
        'pe_avg_5y', 'pe_avg_10y', 'pe_avg_15y', 'pe_avg_20y', 'pe_avg_10y_2019',
        'pe_avg_5y_count', 'pe_avg_10y_count', 'pe_avg_15y_count', 'pe_avg_20y_count',
        'yf_ttm_pe', 'yf_forward_pe',
        'yf_ttm_pe_vs_avg_5y', 'yf_ttm_pe_vs_avg_10y', 'yf_ttm_pe_vs_avg_15y', 'yf_ttm_pe_vs_avg_20y', 'yf_ttm_pe_vs_avg_10y_2019',
        'yf_fwd_pe_vs_avg_5y', 'yf_fwd_pe_vs_avg_10y', 'yf_fwd_pe_vs_avg_15y', 'yf_fwd_pe_vs_avg_20y', 'yf_fwd_pe_vs_avg_10y_2019'
    ]);

    // EV/EBIT-relevante Spalten
    const EV_EBIT_COLUMNS = new Set([
        'ttm_ev_ebit', 'fy_ev_ebit',
        'ev_ebit_avg_5y', 'ev_ebit_avg_10y', 'ev_ebit_avg_15y', 'ev_ebit_avg_20y', 'ev_ebit_avg_10y_2019',
        'ev_ebit_avg_5y_count', 'ev_ebit_avg_10y_count', 'ev_ebit_avg_15y_count', 'ev_ebit_avg_20y_count',
        'ev_ebit_vs_avg_5y', 'ev_ebit_vs_avg_10y', 'ev_ebit_vs_avg_15y', 'ev_ebit_vs_avg_20y', 'ev_ebit_vs_avg_10y_2019'
    ]);

    // Wachstums-relevante Spalten
    const GROWTH_COLUMNS = new Set([
        'revenue_cagr_3y', 'revenue_cagr_5y', 'revenue_cagr_10y',
        'ebit_cagr_3y', 'ebit_cagr_5y', 'ebit_cagr_10y',
        'net_income_cagr_3y', 'net_income_cagr_5y', 'net_income_cagr_10y'
    ]);

    // Margen-relevante Spalten
    const MARGIN_COLUMNS = new Set([
        'profit_margin', 'operating_margin',
        'profit_margin_avg_3y', 'profit_margin_avg_5y', 'profit_margin_avg_10y', 'profit_margin_avg_5y_2019',
        'operating_margin_avg_3y', 'operating_margin_avg_5y', 'operating_margin_avg_10y', 'operating_margin_avg_5y_2019'
    ]);

    // Funktion zum Initialisieren der clickable cells mittels Event Delegation
    function initializeClickableCells() {
        // Event Delegation: Registriere nur einen Handler auf der Tabelle
        const tables = document.querySelectorAll('.stock-table');
        tables.forEach(table => {
            // Prüfe ob bereits initialisiert
            if (table.dataset.cellClicksInitialized) return;
            table.dataset.cellClicksInitialized = 'true';

            table.addEventListener('click', function(e) {
                const cell = e.target.closest('td[data-column]');
                if (!cell) return;

                const columnKey = cell.dataset.column;

                // company_name wird von initializeCompanyNameClicks() behandelt
                if (columnKey === 'company_name') return;

                const row = cell.closest('tr');
                const isin = row?.dataset.isin;
                if (!isin) return;

                // Bestimme Modal-Typ basierend auf Spalte
                if (PE_COLUMNS.has(columnKey)) {
                    openUnifiedModal(isin, 'pe');
                } else if (EV_EBIT_COLUMNS.has(columnKey)) {
                    openUnifiedModal(isin, 'ev_ebit');
                } else if (GROWTH_COLUMNS.has(columnKey)) {
                    openUnifiedModal(isin, 'growth');
                } else if (MARGIN_COLUMNS.has(columnKey)) {
                    openUnifiedModal(isin, 'margins');
                } else if (columnKey === 'price') {
                    openUnifiedModal(isin, 'chart');
                }
            });
        });

        // CSS-Klassen für clickable cells hinzufügen (für Styling)
        document.querySelectorAll('.stock-table tbody td[data-column]').forEach(cell => {
            const columnKey = cell.dataset.column;
            if (columnKey === 'company_name') return;
            if (cell.classList.contains('clickable-cell')) return;

            if (PE_COLUMNS.has(columnKey) ||
                EV_EBIT_COLUMNS.has(columnKey) ||
                GROWTH_COLUMNS.has(columnKey) ||
                MARGIN_COLUMNS.has(columnKey) ||
                columnKey === 'price') {
                cell.classList.add('clickable-cell');
            }
        });
    }

    // Klick auf Tabellenzellen mit Kennzahlen initialisieren
    initializeClickableCells();

    // Helper: Modal-Header konsistent setzen (Ticker | Sektor | Land | FJ)
    function setModalHeader(companyData) {
        if (!companyData) return;
        // Normalisiere verschiedene API-Formate (flat vs nested)
        const name = companyData.name || companyData.company_name || '-';
        const ticker = companyData.ticker || '';
        const sector = companyData.sector || '';
        const country = companyData.country || '';
        const fiscalYear = companyData.fiscal_year_end ? ` | FJ: ${companyData.fiscal_year_end}` : '';

        detailCompanyName.textContent = name;
        const parts = [ticker, sector, country].filter(Boolean);
        detailMeta.textContent = parts.join(' | ') + fiscalYear;
    }

    // Unified Modal öffnen — ersetzt openStockDetail, openCompanyInfo, openCompanyInfoWithTab
    async function openUnifiedModal(isin, tab = 'pe') {
        if (!stockDetailModal) return;

        // Bei neuem ISIN: Cache leeren
        if (modalState.isin !== isin) {
            modalState.isin = isin;
            modalState.cache = {
                detailData: null,
                infoData: null,
                priceData: null,
                earningsData: null,
                dcfData: null
            };
        }

        modalState.activeTab = tab;

        // Modal öffnen mit Ladeindikator
        stockDetailModal.classList.remove('hidden');
        detailCompanyName.textContent = 'Lade...';
        detailMeta.textContent = '';
        detailBody.innerHTML = '<div class="detail-loading">Lade Daten...</div>';

        // Aktiven Tab setzen
        stockDetailModal.querySelectorAll('.detail-tab').forEach(t => {
            t.classList.toggle('active', t.dataset.tab === tab);
        });

        // Content laden via Dispatcher
        loadTabContent(tab);
    }

    // Zentraler Tab-Dispatcher: Prüft Cache, lädt bei Bedarf, rendert
    async function loadTabContent(tab) {
        const isin = modalState.isin;
        if (!isin) return;

        try {
            if (tab === 'pe' || tab === 'ev_ebit' || tab === 'growth' || tab === 'margins') {
                // Diese 4 Tabs teilen sich den /details-Endpoint
                if (!modalState.cache.detailData) {
                    detailBody.innerHTML = '<div class="detail-loading">Lade Daten...</div>';
                    const response = await fetch(`/api/stock/${isin}/details`);
                    if (!response.ok) throw new Error('Fehler beim Laden');
                    modalState.cache.detailData = await response.json();
                }
                const data = modalState.cache.detailData;
                if (tab === 'ev_ebit') renderEvEbitDetail(data);
                else if (tab === 'growth') renderGrowthDetail(data);
                else if (tab === 'margins') renderMarginsDetail(data);
                else renderStockDetail(data);

            } else if (tab === 'info') {
                if (!modalState.cache.infoData) {
                    detailBody.innerHTML = '<div class="detail-loading">Lade Daten...</div>';
                    const response = await fetch(`/api/stock/${isin}/info`);
                    if (!response.ok) throw new Error('Fehler beim Laden');
                    modalState.cache.infoData = await response.json();
                }
                renderCompanyInfo(modalState.cache.infoData);

            } else if (tab === 'chart') {
                if (!modalState.cache.priceData) {
                    detailBody.innerHTML = '<div class="detail-loading">Lade Kursdaten...</div>';
                    const response = await fetch(`/api/stock/${isin}/price-history`);
                    if (!response.ok) throw new Error('Fehler beim Laden');
                    modalState.cache.priceData = await response.json();
                }
                renderPriceChart(modalState.cache.priceData);

            } else if (tab === 'earnings') {
                if (!modalState.cache.earningsData) {
                    detailBody.innerHTML = '<div class="detail-loading">Lade Earnings-Daten...</div>';
                    const response = await fetch(`/api/stock/${isin}/earnings`);
                    if (!response.ok) throw new Error('Fehler beim Laden');
                    modalState.cache.earningsData = await response.json();
                }
                renderEarningsTab(modalState.cache.earningsData);

            } else if (tab === 'dcf') {
                if (!modalState.cache.dcfData) {
                    detailBody.innerHTML = '<div class="detail-loading">Lade DCF-Daten...</div>';
                    const response = await fetch(`/api/stock/${isin}/dcf-data`);
                    if (!response.ok) throw new Error('Fehler beim Laden');
                    modalState.cache.dcfData = await response.json();
                    dcfData = modalState.cache.dcfData;
                    currentScenarioId = null;
                    currentDcfResult = null;
                }
                renderDcfDetail(modalState.cache.dcfData);
            }
        } catch (error) {
            console.error('Fehler:', error);
            detailBody.innerHTML = '<div class="detail-loading">Fehler beim Laden der Daten.</div>';
        }
    }

    function renderStockDetail(data) {
        // Header
        setModalHeader(data.company);

        // Formatierungsfunktionen
        const formatBillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e9).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mrd';
        };

        const formatNumber = (val, decimals = 1) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
        };

        const formatPEAverage = (val, years, count) => {
            if (val === null || val === undefined) {
                return `<span class="pe-unavailable" title="Zu wenig Datenjahre für ${years}-Jahres-Durchschnitt verfügbar">n.v.</span>`;
            }
            const countText = count ? `${count} von ${years} Jahren` : `${years} Jahre`;
            const tooltip = `Durchschnitt über ${countText} (nach Filterung: Negativwerte und Ausreißer >2σ ausgeschlossen)`;
            return `<span class="pe-value-with-tooltip" title="${tooltip}">${val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 })}</span>`;
        };

        const formatEVEBITAverage = (val, years, count) => {
            if (val === null || val === undefined) {
                return `<span class="pe-unavailable" title="Zu wenig Datenjahre für ${years}-Jahres-Durchschnitt verfügbar">n.v.</span>`;
            }
            const countText = count ? `${count} von ${years} Jahren` : `${years} Jahre`;
            const tooltip = `Durchschnitt über ${countText} (nach Filterung: Negativwerte und Ausreißer >2σ ausgeschlossen)`;
            return `<span class="pe-value-with-tooltip" title="${tooltip}">${val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 })}</span>`;
        };

        // TTM Berechnung HTML
        const ttm = data.ttm_calculation;
        let quartersHtml = '';
        if (ttm.quarters && ttm.quarters.length > 0) {
            ttm.quarters.forEach(q => {
                // Jahr aus Datum extrahieren (z.B. "2025-06-30" -> "2025")
                const year = q.date ? q.date.substring(0, 4) : '';
                const periodLabel = year ? `${q.period} ${year}` : q.period;
                quartersHtml += `
                    <div class="ttm-quarter">
                        <span class="ttm-quarter-period">${periodLabel}</span>
                        <span class="ttm-quarter-value">${formatBillions(q.net_income)}</span>
                    </div>
                `;
            });
            quartersHtml += `
                <div class="ttm-quarter ttm-quarter-sum">
                    <span class="ttm-quarter-period">Summe</span>
                    <span class="ttm-quarter-value">${formatBillions(ttm.ttm_net_income)}</span>
                </div>
            `;
        }

        // PE Übersicht mit Abweichungen
        const pe = data.pe_overview || {};

        // Hilfsfunktion für Abweichungs-Formatierung
        const formatDiff = (val) => {
            if (val === null || val === undefined) return '-';
            const sign = val >= 0 ? '+' : '';
            return sign + val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%';
        };

        const getDiffClass = (val) => {
            if (val === null || val === undefined) return '';
            return val < 0 ? 'diff-positive' : 'diff-negative';
        };

        // Tabelle mit Werten und Abweichungen
        let peOverviewHtml = `
            <table class="pe-comparison-table">
                <thead>
                    <tr>
                        <th></th>
                        <th>Wert</th>
                        <th>TTM vs Ø</th>
                        <th>Fwd vs Ø</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td class="pe-label">YF TTM-KGV</td>
                        <td class="pe-value">${formatNumber(pe.yf_ttm_pe)}</td>
                        <td class="pe-diff"></td>
                        <td class="pe-diff"></td>
                    </tr>
                    <tr>
                        <td class="pe-label">YF Forward-KGV</td>
                        <td class="pe-value">${formatNumber(pe.yf_forward_pe)}</td>
                        <td class="pe-diff"></td>
                        <td class="pe-diff"></td>
                    </tr>
                    <tr class="pe-separator">
                        <td colspan="4"></td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 5J</td>
                        <td class="pe-value">${formatPEAverage(pe.pe_avg_5y, 5, pe.pe_avg_5y_count)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_ttm_pe_vs_avg_5y)}">${formatDiff(pe.yf_ttm_pe_vs_avg_5y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_fwd_pe_vs_avg_5y)}">${formatDiff(pe.yf_fwd_pe_vs_avg_5y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 10J</td>
                        <td class="pe-value">${formatPEAverage(pe.pe_avg_10y, 10, pe.pe_avg_10y_count)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_ttm_pe_vs_avg_10y)}">${formatDiff(pe.yf_ttm_pe_vs_avg_10y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_fwd_pe_vs_avg_10y)}">${formatDiff(pe.yf_fwd_pe_vs_avg_10y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 15J</td>
                        <td class="pe-value">${formatPEAverage(pe.pe_avg_15y, 15, pe.pe_avg_15y_count)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_ttm_pe_vs_avg_15y)}">${formatDiff(pe.yf_ttm_pe_vs_avg_15y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_fwd_pe_vs_avg_15y)}">${formatDiff(pe.yf_fwd_pe_vs_avg_15y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 20J</td>
                        <td class="pe-value">${formatPEAverage(pe.pe_avg_20y, 20, pe.pe_avg_20y_count)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_ttm_pe_vs_avg_20y)}">${formatDiff(pe.yf_ttm_pe_vs_avg_20y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_fwd_pe_vs_avg_20y)}">${formatDiff(pe.yf_fwd_pe_vs_avg_20y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 10-19</td>
                        <td class="pe-value">${formatNumber(pe.pe_avg_10y_2019)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_ttm_pe_vs_avg_10y_2019)}">${formatDiff(pe.yf_ttm_pe_vs_avg_10y_2019)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_fwd_pe_vs_avg_10y_2019)}">${formatDiff(pe.yf_fwd_pe_vs_avg_10y_2019)}</td>
                    </tr>
                </tbody>
            </table>
        `;

        // Body HTML aufbauen
        let html = `
            <!-- TTM PE Berechnung + PE Übersicht -->
            <div class="detail-section">
                <div class="detail-section-title">KGV Übersicht</div>
                <div class="pe-section-grid">
                    <div class="ttm-calc-box">
                        <div class="ttm-formula">
                            <div class="ttm-fraction">
                                <div class="ttm-numerator">
                                    <span class="ttm-label">Market Cap</span>
                                    <span class="ttm-value">${formatBillions(ttm.market_cap)}</span>
                                </div>
                                <div class="ttm-denominator">
                                    <span class="ttm-label">TTM Net Income</span>
                                    <span class="ttm-value">${formatBillions(ttm.ttm_net_income)}</span>
                                </div>
                            </div>
                            <span class="ttm-equals">=</span>
                            <span class="ttm-result">${formatNumber(data.current.ttm_pe)}</span>
                        </div>
                        <div class="ttm-quarters">
                            ${quartersHtml}
                        </div>
                    </div>
                    <div class="pe-overview-box">
                        ${peOverviewHtml}
                    </div>
                </div>
            </div>

            <!-- Charts -->
            <div class="detail-charts">
                <div class="chart-container">
                    <div class="chart-title">KGV Verlauf (20 Jahre)</div>
                    <div class="chart-wrapper">
                        <canvas id="pe-chart"></canvas>
                    </div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">Net Income Verlauf</div>
                    <div class="chart-wrapper">
                        <canvas id="income-chart"></canvas>
                    </div>
                </div>
            </div>

            <!-- Income Statement Tabelle -->
            <div class="detail-section">
                <div class="detail-section-title">Income Statement (${data.company.currency || 'EUR'})</div>
                <div class="income-table-container">
                    <table class="income-table">
                        <thead>
                            <tr>
                                <th></th>
                                ${data.income_statement.map(y => `<th>'${String(y.year).slice(-2)}</th>`).join('')}
                                <th class="ttm-col">TTM</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr class="row-revenue">
                                <td>Revenue</td>
                                ${data.income_statement.map(y => `<td>${formatBillions(y.revenue)}</td>`).join('')}
                                <td class="ttm-col">${formatBillions(data.ttm_income_statement?.revenue)}</td>
                            </tr>
                            <tr>
                                <td>Gross Profit</td>
                                ${data.income_statement.map(y => `<td>${formatBillions(y.gross_profit)}</td>`).join('')}
                                <td class="ttm-col">${formatBillions(data.ttm_income_statement?.gross_profit)}</td>
                            </tr>
                            <tr>
                                <td>Operating Inc</td>
                                ${data.income_statement.map(y => `<td>${formatBillions(y.operating_income)}</td>`).join('')}
                                <td class="ttm-col">${formatBillions(data.ttm_income_statement?.operating_income)}</td>
                            </tr>
                            <tr class="row-net-income">
                                <td>Net Income</td>
                                ${data.income_statement.map(y => `<td>${formatBillions(y.net_income)}</td>`).join('')}
                                <td class="ttm-col">${formatBillions(data.ttm_income_statement?.net_income)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        detailBody.innerHTML = html;

        // Charts rendern (nur wenn Chart.js geladen ist)
        if (typeof Chart !== 'undefined') {
            renderPEChart(data.pe_history, data.current_ttm_pe);
            renderIncomeChart(data.income_statement, data.ttm_income_statement);

            // Click-Handler für Chart-Zoom hinzufügen
            setupChartZoomHandlers();
        }
    }

    // Hilfsfunktion: Berechnet das n-te Perzentil eines Arrays
    function calculatePercentile(arr, percentile) {
        const validValues = arr.filter(v => v != null && !isNaN(v) && isFinite(v));
        if (validValues.length === 0) return null;

        const sorted = [...validValues].sort((a, b) => a - b);
        const index = (percentile / 100) * (sorted.length - 1);
        const lower = Math.floor(index);
        const upper = Math.ceil(index);
        const weight = index % 1;

        if (lower === upper) return sorted[lower];
        return sorted[lower] * (1 - weight) + sorted[upper] * weight;
    }

    function calculateIQR(arr) {
        const validValues = arr.filter(v => v != null && !isNaN(v) && isFinite(v));
        if (validValues.length === 0) return null;

        const sorted = [...validValues].sort((a, b) => a - b);
        const q1 = calculatePercentile(sorted, 25);
        const q3 = calculatePercentile(sorted, 75);

        if (q1 === null || q3 === null) return null;

        return {
            q1: q1,
            q3: q3,
            iqr: q3 - q1
        };
    }

    function calculateRobustYMax(arr) {
        const validValues = arr.filter(v => v != null && !isNaN(v) && isFinite(v));
        if (validValues.length === 0) return undefined;

        // Berechne verschiedene statistische Metriken
        const median = calculatePercentile(validValues, 50);
        const p85 = calculatePercentile(validValues, 85);
        const iqrData = calculateIQR(validValues);

        if (!median || !p85 || !iqrData) return undefined;

        // Hybridansatz: Minimum von drei Methoden
        const medianLimit = median * 4;              // Nicht mehr als 4x Median
        const percentileLimit = p85 * 1.2;          // Nicht mehr als 85. Perzentil + 20%
        const iqrLimit = iqrData.q3 + 1.5 * iqrData.iqr;  // IQR-Ausreißergrenze

        // Nimm das Minimum der drei Grenzen (konservativster Ansatz)
        const yMax = Math.min(medianLimit, percentileLimit, iqrLimit);

        // Füge 10% Puffer hinzu für bessere Darstellung
        return yMax * 1.1;
    }

    function renderPEChart(peHistory, currentTtmPe) {
        const ctx = document.getElementById('pe-chart');
        if (!ctx) return;

        // Bestehenden Chart zerstören
        if (peChart) {
            peChart.destroy();
        }

        // Labels und Daten vorbereiten
        const labels = peHistory.map(p => p.year);
        const peData = peHistory.map(p => p.pe);

        // TTM als letzten Punkt hinzufügen
        if (currentTtmPe) {
            labels.push('TTM');
            peData.push(currentTtmPe);
        }

        // Daten für Vollbild-Ansicht speichern
        chartData.pe = { labels, peData, currentTtmPe };

        // Robuste Y-Achsen-Begrenzung mit Hybridansatz berechnen
        const yMax = calculateRobustYMax(peData);

        // Ausreißer identifizieren und unterschiedlich darstellen
        const pointStyles = peData.map(val => (yMax && val > yMax) ? 'triangle' : 'circle');
        const pointRadii = peData.map(val => (yMax && val > yMax) ? 5 : 3);

        // Werte für Anzeige begrenzen, aber echte Werte für Tooltip behalten
        const displayData = peData.map(val => (yMax && val > yMax) ? yMax : val);

        peChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'KGV',
                    data: displayData,
                    borderColor: '#1a1a2e',
                    backgroundColor: 'rgba(26, 26, 46, 0.1)',
                    borderWidth: 2,
                    pointRadius: pointRadii,
                    pointStyle: pointStyles,
                    pointBackgroundColor: '#1a1a2e',
                    fill: true,
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const actualValue = peData[context.dataIndex];
                                const isOutlier = yMax && actualValue > yMax;
                                const valueStr = actualValue != null ? actualValue.toFixed(2) : '-';
                                return `KGV: ${valueStr}${isOutlier ? ' (Ausreißer)' : ''}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 9 } },
                        grid: { display: false }
                    },
                    y: {
                        beginAtZero: true,
                        max: yMax,
                        ticks: { font: { size: 9 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    // =========================================================================
    // Chart Factory: Generische Bar-Chart-Erstellung
    // =========================================================================

    /**
     * Erstellt einen Bar-Chart mit optionalem TTM-Wert.
     * @param {Object} options - Konfigurationsobjekt
     * @param {string} options.canvasId - ID des Canvas-Elements
     * @param {Object} options.chartRef - Referenz auf die Chart-Variable (zur Zerstörung)
     * @param {Array} options.incomeStatement - Array mit Jahreswerten
     * @param {Object} options.ttmIncomeStatement - TTM-Objekt (optional)
     * @param {string} options.dataField - Feld im incomeStatement (z.B. 'net_income')
     * @param {string} options.ttmField - Feld im ttmIncomeStatement (optional, default = dataField)
     * @param {string} options.label - Legende/Label für das Dataset
     * @param {string} options.positiveColor - Farbe für positive Werte (default: '#2d5aa3')
     * @param {string} options.ttmColor - Farbe für TTM-Wert (default: '#1a1a2e')
     * @param {boolean} options.beginAtZero - Y-Achse bei 0 beginnen (default: false)
     * @returns {Chart|null} - Chart-Instanz oder null
     */
    function createFinancialBarChart(options) {
        const {
            canvasId,
            incomeStatement,
            ttmIncomeStatement,
            dataField,
            ttmField = dataField,
            label,
            positiveColor = '#2d5aa3',
            ttmColor = '#1a1a2e',
            negativeColor = '#dc3545',
            beginAtZero = false
        } = options;

        const ctx = document.getElementById(canvasId);
        if (!ctx) return null;

        const labels = incomeStatement.map(y => y.year);
        const data = incomeStatement.map(y => y[dataField] ? y[dataField] / 1e9 : 0);

        // TTM hinzufügen
        if (ttmIncomeStatement?.[ttmField]) {
            labels.push('TTM');
            data.push(ttmIncomeStatement[ttmField] / 1e9);
        }

        // Farben basierend auf positiv/negativ, TTM in separater Farbe
        const colors = data.map((val, idx) => {
            if (labels[idx] === 'TTM') {
                return val >= 0 ? ttmColor : negativeColor;
            }
            return val >= 0 ? positiveColor : negativeColor;
        });

        return new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: label,
                    data: data,
                    backgroundColor: colors,
                    borderRadius: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 9 } },
                        grid: { display: false }
                    },
                    y: {
                        beginAtZero: beginAtZero,
                        ticks: { font: { size: 9 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    function renderIncomeChart(incomeStatement, ttmIncomeStatement) {
        const ctx = document.getElementById('income-chart');
        if (!ctx) return;

        // Bestehenden Chart zerstören
        if (incomeChart) {
            incomeChart.destroy();
        }

        const labels = incomeStatement.map(y => y.year);
        const netIncomeData = incomeStatement.map(y => y.net_income ? y.net_income / 1e9 : 0);

        // TTM hinzufügen
        if (ttmIncomeStatement?.net_income) {
            labels.push('TTM');
            netIncomeData.push(ttmIncomeStatement.net_income / 1e9);
        }

        // Farben basierend auf positiv/negativ, TTM in anderer Farbe
        const colors = netIncomeData.map((val, idx) => {
            if (labels[idx] === 'TTM') {
                return val >= 0 ? '#1a1a2e' : '#dc3545';
            }
            return val >= 0 ? '#2d5aa3' : '#dc3545';
        });

        incomeChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Net Income (Mrd)',
                    data: netIncomeData,
                    backgroundColor: colors,
                    borderRadius: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 9 } },
                        grid: { display: false }
                    },
                    y: {
                        ticks: { font: { size: 9 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    // =========================================================================
    // EV/EBIT Detail Modal
    // =========================================================================

    function renderEvEbitDetail(data) {
        // Header
        setModalHeader(data.company);

        // Formatierungsfunktionen
        const formatBillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e9).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mrd';
        };

        const formatNumber = (val, decimals = 1) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
        };

        const formatEVEBITAverage = (val, years, count) => {
            if (val === null || val === undefined) {
                return `<span class="pe-unavailable" title="Zu wenig Datenjahre für ${years}-Jahres-Durchschnitt verfügbar">n.v.</span>`;
            }
            const countText = count ? `${count} von ${years} Jahren` : `${years} Jahre`;
            const tooltip = `Durchschnitt über ${countText} (nach Filterung: Negativwerte und Ausreißer >2σ ausgeschlossen)`;
            return `<span class="pe-value-with-tooltip" title="${tooltip}">${val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 })}</span>`;
        };

        // EV Berechnung HTML
        const ev = data.ev_calculation || {};
        const marketCap = ev.market_cap || 0;
        const netDebt = ev.net_debt || 0;
        const minorityInterest = ev.minority_interest || 0;
        const enterpriseValue = marketCap + netDebt + minorityInterest;

        let quartersEbitHtml = '';
        if (ev.quarters && ev.quarters.length > 0) {
            ev.quarters.forEach(q => {
                const year = q.date ? q.date.substring(0, 4) : '';
                const periodLabel = year ? `${q.period} ${year}` : q.period;
                quartersEbitHtml += `
                    <div class="ttm-quarter">
                        <span class="ttm-quarter-period">${periodLabel}</span>
                        <span class="ttm-quarter-value">${formatBillions(q.operating_income)}</span>
                    </div>
                `;
            });
            quartersEbitHtml += `
                <div class="ttm-quarter ttm-quarter-sum">
                    <span class="ttm-quarter-period">Summe</span>
                    <span class="ttm-quarter-value">${formatBillions(ev.ttm_ebit)}</span>
                </div>
            `;
        }

        // EV/EBIT Übersicht mit Abweichungen
        const evEbit = data.ev_ebit_overview || {};

        // Hilfsfunktion für Abweichungs-Formatierung
        const formatDiff = (val) => {
            if (val === null || val === undefined) return '-';
            const sign = val >= 0 ? '+' : '';
            return sign + val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%';
        };

        const getDiffClass = (val) => {
            if (val === null || val === undefined) return '';
            return val < 0 ? 'diff-positive' : 'diff-negative';
        };

        // Tabelle mit Werten und Abweichungen
        let evEbitOverviewHtml = `
            <table class="pe-comparison-table">
                <thead>
                    <tr>
                        <th></th>
                        <th>Wert</th>
                        <th>vs Ø</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td class="pe-label">TTM EV/EBIT</td>
                        <td class="pe-value">${formatNumber(evEbit.ttm_ev_ebit)}</td>
                        <td class="pe-diff"></td>
                    </tr>
                    <tr>
                        <td class="pe-label">FY EV/EBIT</td>
                        <td class="pe-value">${formatNumber(evEbit.fy_ev_ebit)}</td>
                        <td class="pe-diff"></td>
                    </tr>
                    <tr class="pe-separator">
                        <td colspan="3"></td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 5J</td>
                        <td class="pe-value">${formatEVEBITAverage(evEbit.ev_ebit_avg_5y, 5, evEbit.ev_ebit_avg_5y_count)}</td>
                        <td class="pe-diff ${getDiffClass(evEbit.ev_ebit_vs_avg_5y)}">${formatDiff(evEbit.ev_ebit_vs_avg_5y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 10J</td>
                        <td class="pe-value">${formatEVEBITAverage(evEbit.ev_ebit_avg_10y, 10, evEbit.ev_ebit_avg_10y_count)}</td>
                        <td class="pe-diff ${getDiffClass(evEbit.ev_ebit_vs_avg_10y)}">${formatDiff(evEbit.ev_ebit_vs_avg_10y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 15J</td>
                        <td class="pe-value">${formatEVEBITAverage(evEbit.ev_ebit_avg_15y, 15, evEbit.ev_ebit_avg_15y_count)}</td>
                        <td class="pe-diff ${getDiffClass(evEbit.ev_ebit_vs_avg_15y)}">${formatDiff(evEbit.ev_ebit_vs_avg_15y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 20J</td>
                        <td class="pe-value">${formatEVEBITAverage(evEbit.ev_ebit_avg_20y, 20, evEbit.ev_ebit_avg_20y_count)}</td>
                        <td class="pe-diff ${getDiffClass(evEbit.ev_ebit_vs_avg_20y)}">${formatDiff(evEbit.ev_ebit_vs_avg_20y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 10-19</td>
                        <td class="pe-value">${formatNumber(evEbit.ev_ebit_avg_10y_2019)}</td>
                        <td class="pe-diff ${getDiffClass(evEbit.ev_ebit_vs_avg_10y_2019)}">${formatDiff(evEbit.ev_ebit_vs_avg_10y_2019)}</td>
                    </tr>
                </tbody>
            </table>
        `;

        // Body HTML aufbauen
        let html = `
            <!-- EV/EBIT Berechnung + Übersicht -->
            <div class="detail-section">
                <div class="detail-section-title">EV/EBIT Übersicht</div>
                <div class="pe-section-grid">
                    <div class="ev-calc-box">
                        <div class="ev-formula">
                            <div class="ev-components">
                                <div class="ev-component">
                                    <span class="ev-label">Market Cap</span>
                                    <span class="ev-value">${formatBillions(marketCap)}</span>
                                </div>
                                <div class="ev-component">
                                    <span class="ev-label">+ Net Debt</span>
                                    <span class="ev-value">${formatBillions(netDebt)}</span>
                                </div>
                                <div class="ev-component">
                                    <span class="ev-label">+ Minority Int.</span>
                                    <span class="ev-value">${formatBillions(minorityInterest)}</span>
                                </div>
                                <div class="ev-component ev-total">
                                    <span class="ev-label">= EV</span>
                                    <span class="ev-value">${formatBillions(enterpriseValue)}</span>
                                </div>
                            </div>
                            <div class="ev-divider">÷</div>
                            <div class="ev-ebit-section">
                                <div class="ev-component">
                                    <span class="ev-label">TTM EBIT</span>
                                    <span class="ev-value">${formatBillions(ev.ttm_ebit)}</span>
                                </div>
                            </div>
                            <div class="ev-divider">=</div>
                            <div class="ev-result">${formatNumber(data.current_ttm_ev_ebit)}</div>
                        </div>
                        <div class="ttm-quarters">
                            ${quartersEbitHtml}
                        </div>
                    </div>
                    <div class="pe-overview-box">
                        ${evEbitOverviewHtml}
                    </div>
                </div>
            </div>

            <!-- Charts -->
            <div class="detail-charts">
                <div class="chart-container">
                    <div class="chart-title">EV/EBIT Verlauf (20 Jahre)</div>
                    <div class="chart-wrapper">
                        <canvas id="ev-ebit-chart"></canvas>
                    </div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">EBIT Verlauf</div>
                    <div class="chart-wrapper">
                        <canvas id="ebit-chart"></canvas>
                    </div>
                </div>
            </div>

            <!-- Income Statement Tabelle -->
            <div class="detail-section">
                <div class="detail-section-title">Income Statement (${data.company.currency || 'EUR'})</div>
                <div class="income-table-container">
                    <table class="income-table">
                        <thead>
                            <tr>
                                <th></th>
                                ${data.income_statement.map(y => `<th>'${String(y.year).slice(-2)}</th>`).join('')}
                                <th class="ttm-col">TTM</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr class="row-revenue">
                                <td>Revenue</td>
                                ${data.income_statement.map(y => `<td>${formatBillions(y.revenue)}</td>`).join('')}
                                <td class="ttm-col">${formatBillions(data.ttm_income_statement?.revenue)}</td>
                            </tr>
                            <tr class="row-net-income">
                                <td>EBIT</td>
                                ${data.income_statement.map(y => `<td>${formatBillions(y.operating_income)}</td>`).join('')}
                                <td class="ttm-col">${formatBillions(data.ttm_income_statement?.operating_income)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        detailBody.innerHTML = html;

        // Charts rendern
        if (typeof Chart !== 'undefined') {
            renderEvEbitChart(data.ev_ebit_history, data.current_ttm_ev_ebit);
            renderEbitChart(data.income_statement, data.ttm_income_statement);

            // Click-Handler für Chart-Zoom hinzufügen
            setupChartZoomHandlers();
        }
    }

    function renderEvEbitChart(evEbitHistory, currentTtmEvEbit) {
        const ctx = document.getElementById('ev-ebit-chart');
        if (!ctx) return;

        if (evEbitChart) {
            evEbitChart.destroy();
        }

        const labels = evEbitHistory.map(p => p.year);
        const evEbitData = evEbitHistory.map(p => p.ev_ebit);

        if (currentTtmEvEbit) {
            labels.push('TTM');
            evEbitData.push(currentTtmEvEbit);
        }

        // Daten für Vollbild-Ansicht speichern
        chartData.evEbit = { labels, evEbitData, currentTtmEvEbit };

        // Robuste Y-Achsen-Begrenzung mit Hybridansatz berechnen
        const yMax = calculateRobustYMax(evEbitData);

        // Ausreißer identifizieren und unterschiedlich darstellen
        const pointStyles = evEbitData.map(val => (yMax && val > yMax) ? 'triangle' : 'circle');
        const pointRadii = evEbitData.map(val => (yMax && val > yMax) ? 5 : 3);

        // Werte für Anzeige begrenzen, aber echte Werte für Tooltip behalten
        const displayData = evEbitData.map(val => (yMax && val > yMax) ? yMax : val);

        evEbitChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'EV/EBIT',
                    data: displayData,
                    borderColor: '#2d5aa3',
                    backgroundColor: 'rgba(45, 90, 163, 0.1)',
                    borderWidth: 2,
                    pointRadius: pointRadii,
                    pointStyle: pointStyles,
                    pointBackgroundColor: '#2d5aa3',
                    fill: true,
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const actualValue = evEbitData[context.dataIndex];
                                const isOutlier = yMax && actualValue > yMax;
                                const valueStr = actualValue != null ? actualValue.toFixed(2) : '-';
                                return `EV/EBIT: ${valueStr}${isOutlier ? ' (Ausreißer)' : ''}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 9 } },
                        grid: { display: false }
                    },
                    y: {
                        beginAtZero: true,
                        max: yMax,
                        ticks: { font: { size: 9 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    function renderEbitChart(incomeStatement, ttmIncomeStatement) {
        const ctx = document.getElementById('ebit-chart');
        if (!ctx) return;

        if (ebitChart) {
            ebitChart.destroy();
        }

        const labels = incomeStatement.map(y => y.year);
        const ebitData = incomeStatement.map(y => y.operating_income ? y.operating_income / 1e9 : 0);

        if (ttmIncomeStatement?.operating_income) {
            labels.push('TTM');
            ebitData.push(ttmIncomeStatement.operating_income / 1e9);
        }

        const colors = ebitData.map((val, idx) => {
            if (labels[idx] === 'TTM') {
                return val >= 0 ? '#1a1a2e' : '#dc3545';
            }
            return val >= 0 ? '#2d5aa3' : '#dc3545';
        });

        ebitChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'EBIT (Mrd)',
                    data: ebitData,
                    backgroundColor: colors,
                    borderRadius: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 9 } },
                        grid: { display: false }
                    },
                    y: {
                        ticks: { font: { size: 9 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    // =========================================================================
    // Vollbild-Chart-Funktionalität
    // =========================================================================
    const chartFullscreenModal = document.getElementById('chart-fullscreen-modal');
    const chartFullscreenTitle = document.getElementById('chart-fullscreen-title');
    const chartFullscreenCanvas = document.getElementById('chart-fullscreen-canvas');

    function setupChartZoomHandlers() {
        // Click-Handler für P/E Chart
        const peChartWrapper = document.querySelector('#pe-chart')?.closest('.chart-wrapper');
        if (peChartWrapper) {
            peChartWrapper.style.cursor = 'zoom-in';
            peChartWrapper.onclick = () => openChartFullscreen('pe', 'KGV Verlauf (20 Jahre)');
        }

        // Click-Handler für EV/EBIT Chart
        const evEbitChartWrapper = document.querySelector('#ev-ebit-chart')?.closest('.chart-wrapper');
        if (evEbitChartWrapper) {
            evEbitChartWrapper.style.cursor = 'zoom-in';
            evEbitChartWrapper.onclick = () => openChartFullscreen('evEbit', 'EV/EBIT Verlauf (20 Jahre)');
        }
    }

    function openChartFullscreen(chartType, title) {
        const data = chartData[chartType];
        if (!data) return;

        chartFullscreenTitle.textContent = title;
        chartFullscreenModal.classList.remove('hidden');

        // Bestehenden Chart zerstören
        if (fullscreenChart) {
            fullscreenChart.destroy();
        }

        // Chart im Vollbild rendern
        if (chartType === 'pe') {
            renderFullscreenPEChart(data);
        } else if (chartType === 'evEbit') {
            renderFullscreenEvEbitChart(data);
        }
    }

    function renderFullscreenPEChart(data) {
        const { labels, peData } = data;

        // Robuste Y-Achsen-Begrenzung mit Hybridansatz berechnen
        const yMax = calculateRobustYMax(peData);

        // Ausreißer identifizieren
        const pointStyles = peData.map(val => (yMax && val > yMax) ? 'triangle' : 'circle');
        const pointRadii = peData.map(val => (yMax && val > yMax) ? 6 : 4);
        const displayData = peData.map(val => (yMax && val > yMax) ? yMax : val);

        fullscreenChart = new Chart(chartFullscreenCanvas, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'KGV',
                    data: displayData,
                    borderColor: '#1a1a2e',
                    backgroundColor: 'rgba(26, 26, 46, 0.1)',
                    borderWidth: 3,
                    pointRadius: pointRadii,
                    pointStyle: pointStyles,
                    pointBackgroundColor: '#1a1a2e',
                    fill: true,
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const actualValue = peData[context.dataIndex];
                                const isOutlier = yMax && actualValue > yMax;
                                const valueStr = actualValue != null ? actualValue.toFixed(2) : '-';
                                return `KGV: ${valueStr}${isOutlier ? ' (Ausreißer)' : ''}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 14 } },
                        grid: { display: false }
                    },
                    y: {
                        beginAtZero: true,
                        max: yMax,
                        ticks: { font: { size: 14 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    function renderFullscreenEvEbitChart(data) {
        const { labels, evEbitData } = data;

        // Robuste Y-Achsen-Begrenzung mit Hybridansatz berechnen
        const yMax = calculateRobustYMax(evEbitData);

        // Ausreißer identifizieren
        const pointStyles = evEbitData.map(val => (yMax && val > yMax) ? 'triangle' : 'circle');
        const pointRadii = evEbitData.map(val => (yMax && val > yMax) ? 6 : 4);
        const displayData = evEbitData.map(val => (yMax && val > yMax) ? yMax : val);

        fullscreenChart = new Chart(chartFullscreenCanvas, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'EV/EBIT',
                    data: displayData,
                    borderColor: '#2d5aa3',
                    backgroundColor: 'rgba(45, 90, 163, 0.1)',
                    borderWidth: 3,
                    pointRadius: pointRadii,
                    pointStyle: pointStyles,
                    pointBackgroundColor: '#2d5aa3',
                    fill: true,
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const actualValue = evEbitData[context.dataIndex];
                                const isOutlier = yMax && actualValue > yMax;
                                const valueStr = actualValue != null ? actualValue.toFixed(2) : '-';
                                return `EV/EBIT: ${valueStr}${isOutlier ? ' (Ausreißer)' : ''}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 14 } },
                        grid: { display: false }
                    },
                    y: {
                        beginAtZero: true,
                        max: yMax,
                        ticks: { font: { size: 14 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    // Modal-Close-Handler für Vollbild-Chart
    if (chartFullscreenModal) {
        chartFullscreenModal.querySelectorAll('.modal-close').forEach(btn => {
            btn.addEventListener('click', function() {
                chartFullscreenModal.classList.add('hidden');
                if (fullscreenChart) {
                    fullscreenChart.destroy();
                    fullscreenChart = null;
                }
            });
        });

        // Schließen beim Klick auf Hintergrund
        chartFullscreenModal.addEventListener('click', function(e) {
            if (e.target === chartFullscreenModal) {
                chartFullscreenModal.classList.add('hidden');
                if (fullscreenChart) {
                    fullscreenChart.destroy();
                    fullscreenChart = null;
                }
            }
        });
    }

    // =========================================================================
    // Wachstum Detail Modal
    // =========================================================================
    let revenueChart = null;
    let netIncomeChart = null;

    function renderGrowthDetail(data) {
        // Header
        setModalHeader(data.company);

        // Formatierungsfunktionen
        const formatBillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e9).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mrd';
        };

        const formatPercent = (val) => {
            if (val === null || val === undefined) return '-';
            const sign = val >= 0 ? '+' : '';
            return sign + val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%';
        };

        const formatPercentClass = (val) => {
            if (val === null || val === undefined) return '';
            return val >= 0 ? 'positive' : 'negative';
        };

        // CAGR Übersicht
        const growth = data.growth_overview || {};

        // YoY-Wachstum berechnen für die Tabelle
        const incomeData = data.income_statement || [];
        const growthRows = [];

        for (let i = 0; i < incomeData.length; i++) {
            const curr = incomeData[i];
            const prev = i > 0 ? incomeData[i - 1] : null;

            let revenueGrowth = null;
            let netIncomeGrowth = null;
            let profitMargin = null;

            if (prev && prev.revenue && curr.revenue) {
                revenueGrowth = ((curr.revenue - prev.revenue) / Math.abs(prev.revenue)) * 100;
            }
            if (prev && prev.net_income && curr.net_income && prev.net_income !== 0) {
                netIncomeGrowth = ((curr.net_income - prev.net_income) / Math.abs(prev.net_income)) * 100;
            }
            if (curr.revenue && curr.net_income) {
                profitMargin = (curr.net_income / curr.revenue) * 100;
            }

            growthRows.push({
                year: curr.year,
                revenue: curr.revenue,
                revenueGrowth: revenueGrowth,
                netIncome: curr.net_income,
                netIncomeGrowth: netIncomeGrowth,
                profitMargin: profitMargin
            });
        }

        // TTM hinzufügen falls vorhanden
        const ttm = data.ttm_income_statement;
        if (ttm && ttm.revenue) {
            const lastFY = incomeData.length > 0 ? incomeData[incomeData.length - 1] : null;
            let ttmRevenueGrowth = null;
            let ttmNetIncomeGrowth = null;
            let ttmProfitMargin = null;

            if (lastFY && lastFY.revenue) {
                ttmRevenueGrowth = ((ttm.revenue - lastFY.revenue) / Math.abs(lastFY.revenue)) * 100;
            }
            if (lastFY && lastFY.net_income && ttm.net_income && lastFY.net_income !== 0) {
                ttmNetIncomeGrowth = ((ttm.net_income - lastFY.net_income) / Math.abs(lastFY.net_income)) * 100;
            }
            if (ttm.revenue && ttm.net_income) {
                ttmProfitMargin = (ttm.net_income / ttm.revenue) * 100;
            }

            growthRows.push({
                year: 'TTM',
                revenue: ttm.revenue,
                revenueGrowth: ttmRevenueGrowth,
                netIncome: ttm.net_income,
                netIncomeGrowth: ttmNetIncomeGrowth,
                profitMargin: ttmProfitMargin
            });
        }

        // Body HTML aufbauen
        let html = `
            <!-- CAGR Übersicht -->
            <div class="detail-section">
                <div class="detail-section-title">CAGR Übersicht</div>
                <div class="cagr-overview-box">
                    <table class="cagr-table">
                        <thead>
                            <tr>
                                <th></th>
                                <th>3 Jahre</th>
                                <th>5 Jahre</th>
                                <th>10 Jahre</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td class="cagr-label">Umsatz</td>
                                <td class="${formatPercentClass(growth.revenue_cagr_3y)}">${formatPercent(growth.revenue_cagr_3y)}</td>
                                <td class="${formatPercentClass(growth.revenue_cagr_5y)}">${formatPercent(growth.revenue_cagr_5y)}</td>
                                <td class="${formatPercentClass(growth.revenue_cagr_10y)}">${formatPercent(growth.revenue_cagr_10y)}</td>
                            </tr>
                            <tr>
                                <td class="cagr-label">EBIT</td>
                                <td class="${formatPercentClass(growth.ebit_cagr_3y)}">${formatPercent(growth.ebit_cagr_3y)}</td>
                                <td class="${formatPercentClass(growth.ebit_cagr_5y)}">${formatPercent(growth.ebit_cagr_5y)}</td>
                                <td class="${formatPercentClass(growth.ebit_cagr_10y)}">${formatPercent(growth.ebit_cagr_10y)}</td>
                            </tr>
                            <tr>
                                <td class="cagr-label">Gewinn</td>
                                <td class="${formatPercentClass(growth.net_income_cagr_3y)}">${formatPercent(growth.net_income_cagr_3y)}</td>
                                <td class="${formatPercentClass(growth.net_income_cagr_5y)}">${formatPercent(growth.net_income_cagr_5y)}</td>
                                <td class="${formatPercentClass(growth.net_income_cagr_10y)}">${formatPercent(growth.net_income_cagr_10y)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Charts -->
            <div class="detail-charts">
                <div class="chart-container">
                    <div class="chart-title">Umsatz-Entwicklung</div>
                    <div class="chart-wrapper">
                        <canvas id="revenue-chart"></canvas>
                    </div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">Gewinn-Entwicklung</div>
                    <div class="chart-wrapper">
                        <canvas id="net-income-chart"></canvas>
                    </div>
                </div>
            </div>

            <!-- Wachstum pro Jahr Tabelle -->
            <div class="detail-section">
                <div class="detail-section-title">Wachstum pro Jahr (${data.company.currency || 'EUR'})</div>
                <div class="income-table-container">
                    <table class="income-table growth-table">
                        <thead>
                            <tr>
                                <th>Jahr</th>
                                <th class="num">Umsatz</th>
                                <th class="num">YoY</th>
                                <th class="num">Gewinn</th>
                                <th class="num">YoY</th>
                                <th class="num">Marge</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${growthRows.map(row => `
                                <tr class="${row.year === 'TTM' ? 'ttm-row' : ''}">
                                    <td>${row.year === 'TTM' ? 'TTM' : "'" + String(row.year).slice(-2)}</td>
                                    <td class="num">${formatBillions(row.revenue)}</td>
                                    <td class="num ${formatPercentClass(row.revenueGrowth)}">${row.revenueGrowth !== null ? formatPercent(row.revenueGrowth) : '-'}</td>
                                    <td class="num">${formatBillions(row.netIncome)}</td>
                                    <td class="num ${formatPercentClass(row.netIncomeGrowth)}">${row.netIncomeGrowth !== null ? formatPercent(row.netIncomeGrowth) : '-'}</td>
                                    <td class="num">${row.profitMargin !== null ? row.profitMargin.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%' : '-'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        detailBody.innerHTML = html;

        // Charts rendern
        if (typeof Chart !== 'undefined') {
            renderRevenueChart(data.income_statement, data.ttm_income_statement);
            renderNetIncomeGrowthChart(data.income_statement, data.ttm_income_statement);
        }
    }

    function renderRevenueChart(incomeStatement, ttmIncomeStatement) {
        const ctx = document.getElementById('revenue-chart');
        if (!ctx) return;

        if (revenueChart) {
            revenueChart.destroy();
        }

        const labels = incomeStatement.map(y => y.year);
        const revenueData = incomeStatement.map(y => y.revenue ? y.revenue / 1e9 : 0);

        if (ttmIncomeStatement?.revenue) {
            labels.push('TTM');
            revenueData.push(ttmIncomeStatement.revenue / 1e9);
        }

        const colors = revenueData.map((val, idx) => {
            if (labels[idx] === 'TTM') {
                return '#1a1a2e';
            }
            return '#4a9d5b';
        });

        revenueChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Umsatz (Mrd)',
                    data: revenueData,
                    backgroundColor: colors,
                    borderRadius: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 9 } },
                        grid: { display: false }
                    },
                    y: {
                        beginAtZero: true,
                        ticks: { font: { size: 9 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    function renderNetIncomeGrowthChart(incomeStatement, ttmIncomeStatement) {
        const ctx = document.getElementById('net-income-chart');
        if (!ctx) return;

        if (netIncomeChart) {
            netIncomeChart.destroy();
        }

        const labels = incomeStatement.map(y => y.year);
        const netIncomeData = incomeStatement.map(y => y.net_income ? y.net_income / 1e9 : 0);

        if (ttmIncomeStatement?.net_income) {
            labels.push('TTM');
            netIncomeData.push(ttmIncomeStatement.net_income / 1e9);
        }

        const colors = netIncomeData.map((val, idx) => {
            if (labels[idx] === 'TTM') {
                return val >= 0 ? '#1a1a2e' : '#dc3545';
            }
            return val >= 0 ? '#2d5aa3' : '#dc3545';
        });

        netIncomeChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Gewinn (Mrd)',
                    data: netIncomeData,
                    backgroundColor: colors,
                    borderRadius: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 9 } },
                        grid: { display: false }
                    },
                    y: {
                        ticks: { font: { size: 9 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    // =========================================================================
    // Margen Detail Modal
    // =========================================================================
    let marginsLineChart = null;
    let marginsBarChart = null;

    function renderMarginsDetail(data) {
        // Header
        setModalHeader(data.company);

        // Formatierungsfunktionen
        const formatBillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e9).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mrd';
        };

        const formatPercent = (val) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%';
        };

        // Margen Übersicht
        const margins = data.margins_overview || {};

        // Margen pro Jahr berechnen aus income_statement
        const incomeData = data.income_statement || [];
        const marginRows = [];

        for (let i = 0; i < incomeData.length; i++) {
            const curr = incomeData[i];

            let grossMargin = null;
            let operatingMargin = null;
            let profitMargin = null;

            if (curr.revenue && curr.gross_profit) {
                grossMargin = (curr.gross_profit / curr.revenue) * 100;
            }
            if (curr.revenue && curr.operating_income) {
                operatingMargin = (curr.operating_income / curr.revenue) * 100;
            }
            if (curr.revenue && curr.net_income) {
                profitMargin = (curr.net_income / curr.revenue) * 100;
            }

            marginRows.push({
                year: curr.year,
                revenue: curr.revenue,
                grossProfit: curr.gross_profit,
                grossMargin: grossMargin,
                operatingIncome: curr.operating_income,
                operatingMargin: operatingMargin,
                netIncome: curr.net_income,
                profitMargin: profitMargin
            });
        }

        // TTM hinzufügen falls vorhanden
        const ttm = data.ttm_income_statement;
        if (ttm && ttm.revenue) {
            let ttmGrossMargin = null;
            let ttmOperatingMargin = null;
            let ttmProfitMargin = null;

            if (ttm.gross_profit) {
                ttmGrossMargin = (ttm.gross_profit / ttm.revenue) * 100;
            }
            if (ttm.operating_income) {
                ttmOperatingMargin = (ttm.operating_income / ttm.revenue) * 100;
            }
            if (ttm.net_income) {
                ttmProfitMargin = (ttm.net_income / ttm.revenue) * 100;
            }

            marginRows.push({
                year: 'TTM',
                revenue: ttm.revenue,
                grossProfit: ttm.gross_profit,
                grossMargin: ttmGrossMargin,
                operatingIncome: ttm.operating_income,
                operatingMargin: ttmOperatingMargin,
                netIncome: ttm.net_income,
                profitMargin: ttmProfitMargin
            });
        }

        // Aktuelle Bruttomarge aus letztem FY berechnen
        const lastFY = incomeData.length > 0 ? incomeData[incomeData.length - 1] : null;
        let currentGrossMargin = null;
        if (lastFY && lastFY.revenue && lastFY.gross_profit) {
            currentGrossMargin = (lastFY.gross_profit / lastFY.revenue) * 100;
        }

        // Body HTML aufbauen
        let html = `
            <!-- Margen Übersicht -->
            <div class="detail-section">
                <div class="detail-section-title">Margen Übersicht</div>
                <div class="cagr-overview-box">
                    <table class="cagr-table margins-table">
                        <thead>
                            <tr>
                                <th></th>
                                <th>Aktuell</th>
                                <th>Ø 3J</th>
                                <th>Ø 5J</th>
                                <th>Ø 10J</th>
                                <th>Ø 15-19</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td class="cagr-label">Bruttomarge</td>
                                <td>${formatPercent(currentGrossMargin)}</td>
                                <td>-</td>
                                <td>-</td>
                                <td>-</td>
                                <td>-</td>
                            </tr>
                            <tr>
                                <td class="cagr-label">Op. Marge</td>
                                <td>${formatPercent(margins.operating_margin)}</td>
                                <td>${formatPercent(margins.operating_margin_avg_3y)}</td>
                                <td>${formatPercent(margins.operating_margin_avg_5y)}</td>
                                <td>${formatPercent(margins.operating_margin_avg_10y)}</td>
                                <td>${formatPercent(margins.operating_margin_avg_5y_2019)}</td>
                            </tr>
                            <tr>
                                <td class="cagr-label">Gewinnmarge</td>
                                <td>${formatPercent(margins.profit_margin)}</td>
                                <td>${formatPercent(margins.profit_margin_avg_3y)}</td>
                                <td>${formatPercent(margins.profit_margin_avg_5y)}</td>
                                <td>${formatPercent(margins.profit_margin_avg_10y)}</td>
                                <td>${formatPercent(margins.profit_margin_avg_5y_2019)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Charts -->
            <div class="detail-charts">
                <div class="chart-container">
                    <div class="chart-title">Margen-Entwicklung</div>
                    <div class="chart-wrapper">
                        <canvas id="margins-line-chart"></canvas>
                    </div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">Umsatz & Gewinn</div>
                    <div class="chart-wrapper">
                        <canvas id="margins-bar-chart"></canvas>
                    </div>
                </div>
            </div>

            <!-- Margen pro Jahr Tabelle -->
            <div class="detail-section">
                <div class="detail-section-title">Margen pro Jahr (${data.company.currency || 'EUR'})</div>
                <div class="income-table-container">
                    <table class="income-table growth-table">
                        <thead>
                            <tr>
                                <th>Jahr</th>
                                <th class="num">Umsatz</th>
                                <th class="num">Brutto</th>
                                <th class="num">Brutto%</th>
                                <th class="num">Op.Inc</th>
                                <th class="num">Op.%</th>
                                <th class="num">Gewinn%</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${marginRows.map(row => `
                                <tr class="${row.year === 'TTM' ? 'ttm-row' : ''}">
                                    <td>${row.year === 'TTM' ? 'TTM' : "'" + String(row.year).slice(-2)}</td>
                                    <td class="num">${formatBillions(row.revenue)}</td>
                                    <td class="num">${formatBillions(row.grossProfit)}</td>
                                    <td class="num">${formatPercent(row.grossMargin)}</td>
                                    <td class="num">${formatBillions(row.operatingIncome)}</td>
                                    <td class="num">${formatPercent(row.operatingMargin)}</td>
                                    <td class="num">${formatPercent(row.profitMargin)}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        detailBody.innerHTML = html;

        // Charts rendern
        if (typeof Chart !== 'undefined') {
            renderMarginsLineChart(marginRows);
            renderMarginsBarChart(data.income_statement, data.ttm_income_statement);
        }
    }

    function renderMarginsLineChart(marginRows) {
        const ctx = document.getElementById('margins-line-chart');
        if (!ctx) return;

        if (marginsLineChart) {
            marginsLineChart.destroy();
        }

        const labels = marginRows.map(r => r.year === 'TTM' ? 'TTM' : r.year);
        const grossData = marginRows.map(r => r.grossMargin);
        const operatingData = marginRows.map(r => r.operatingMargin);
        const profitData = marginRows.map(r => r.profitMargin);

        marginsLineChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Bruttomarge',
                        data: grossData,
                        borderColor: '#4a9d5b',
                        backgroundColor: 'transparent',
                        borderWidth: 2,
                        pointRadius: 2,
                        tension: 0.1
                    },
                    {
                        label: 'Op. Marge',
                        data: operatingData,
                        borderColor: '#2d5aa3',
                        backgroundColor: 'transparent',
                        borderWidth: 2,
                        pointRadius: 2,
                        tension: 0.1
                    },
                    {
                        label: 'Gewinnmarge',
                        data: profitData,
                        borderColor: '#1a1a2e',
                        backgroundColor: 'transparent',
                        borderWidth: 2,
                        pointRadius: 2,
                        tension: 0.1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: { size: 9 },
                            padding: 8
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 9 } },
                        grid: { display: false }
                    },
                    y: {
                        ticks: {
                            font: { size: 9 },
                            callback: function(value) {
                                return value + '%';
                            }
                        },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    function renderMarginsBarChart(incomeStatement, ttmIncomeStatement) {
        const ctx = document.getElementById('margins-bar-chart');
        if (!ctx) return;

        if (marginsBarChart) {
            marginsBarChart.destroy();
        }

        const labels = incomeStatement.map(y => y.year);
        const revenueData = incomeStatement.map(y => y.revenue ? y.revenue / 1e9 : 0);
        const netIncomeData = incomeStatement.map(y => y.net_income ? y.net_income / 1e9 : 0);

        if (ttmIncomeStatement?.revenue) {
            labels.push('TTM');
            revenueData.push(ttmIncomeStatement.revenue / 1e9);
            netIncomeData.push(ttmIncomeStatement.net_income ? ttmIncomeStatement.net_income / 1e9 : 0);
        }

        marginsBarChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Umsatz',
                        data: revenueData,
                        backgroundColor: '#4a9d5b',
                        borderRadius: 2
                    },
                    {
                        label: 'Gewinn',
                        data: netIncomeData,
                        backgroundColor: '#1a1a2e',
                        borderRadius: 2
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: { size: 9 },
                            padding: 8
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 9 } },
                        grid: { display: false }
                    },
                    y: {
                        ticks: { font: { size: 9 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    // =========================================================================
    // Company Info / Chart / Earnings Render-Funktionen (jetzt im Unified Modal)
    // =========================================================================
    let priceChart = null;

    function renderCompanyInfo(data) {
        // Header (flat format from /info endpoint)
        setModalHeader({
            name: data.company_name,
            ticker: data.ticker,
            sector: data.sector,
            country: data.country,
            fiscal_year_end: data.fiscal_year_end
        });

        // Body: Beschreibung
        let html = '<div class="company-description">';

        if (data.description) {
            html += `<p>${data.description}</p>`;
        } else {
            html += '<p class="empty-state">Keine Beschreibung verfügbar.</p>';
        }

        // Weitere Infos
        html += '<div class="company-meta-info">';
        if (data.industry) {
            html += `<p><strong>Branche:</strong> ${data.industry}</p>`;
        }
        if (data.stock_index) {
            html += `<p><strong>Index:</strong> ${data.stock_index}</p>`;
        }
        if (data.currency) {
            html += `<p><strong>Währung:</strong> ${data.currency}</p>`;
        }
        if (data.market_cap) {
            const marketCapFormatted = (data.market_cap / 1e9).toLocaleString('de-DE', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            }) + ' Mrd.';
            html += `<p><strong>Marktkapitalisierung:</strong> ${marketCapFormatted}</p>`;
        }
        if (data.next_earnings_date) {
            const earningsDate = new Date(data.next_earnings_date);
            const earningsFormatted = earningsDate.toLocaleDateString('de-DE', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric'
            });
            html += `<p><strong>Nächster Earnings-Termin:</strong> ${earningsFormatted}</p>`;
        }
        html += '</div>';

        html += '</div>';
        detailBody.innerHTML = html;
    }

    function renderPriceChart(data) {
        // Header aktualisieren
        setModalHeader(data.company);

        const formatCurrency = (val, currency) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' ' + (currency || 'EUR');
        };

        const formatPercent = (val, showSign = false) => {
            if (val === null || val === undefined) return '-';
            const sign = showSign && val >= 0 ? '+' : '';
            return sign + val.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + '%';
        };

        const getPerformanceClass = (val) => {
            if (val === null || val === undefined) return '';
            return val >= 0 ? 'perf-positive' : 'perf-negative';
        };

        // Performance-Badges
        const perf = data.performance || {};
        let perfHtml = '<div class="price-performance-badges">';
        const perfLabels = {
            '1m': '1M', '3m': '3M', '6m': '6M',
            '1y': '1J', '3y': '3J', '5y': '5J', 'ytd': 'YTD'
        };
        for (const [key, label] of Object.entries(perfLabels)) {
            if (perf[key] !== undefined) {
                perfHtml += `<span class="perf-badge ${getPerformanceClass(perf[key])}">${label}: ${formatPercent(perf[key], true)}</span>`;
            }
        }
        perfHtml += '</div>';

        // Zeitraum-Buttons für Chart
        let html = `
            <div class="price-chart-container">
                <div class="price-chart-header">
                    <div class="price-current">
                        <span class="price-value">${formatCurrency(data.current.price, data.company.currency)}</span>
                        <span class="price-date">${data.current.price_date ? new Date(data.current.price_date).toLocaleDateString('de-DE') : '-'}</span>
                    </div>
                    ${perfHtml}
                </div>
                <div class="price-chart-timeframe">
                    <button class="timeframe-btn" data-range="1m">1M</button>
                    <button class="timeframe-btn" data-range="3m">3M</button>
                    <button class="timeframe-btn" data-range="6m">6M</button>
                    <button class="timeframe-btn active" data-range="1y">1J</button>
                    <button class="timeframe-btn" data-range="3y">3J</button>
                    <button class="timeframe-btn" data-range="5y">5J</button>
                    <button class="timeframe-btn" data-range="all">Max</button>
                </div>
                <div class="price-chart-wrapper">
                    <canvas id="price-chart-canvas"></canvas>
                </div>
            </div>
        `;

        detailBody.innerHTML = html;

        // Chart rendern
        if (typeof Chart !== 'undefined' && data.prices && data.prices.length > 0) {
            setTimeout(() => {
                renderPriceChartCanvas(data.prices, '1y');

                // Timeframe-Buttons Event-Listener
                document.querySelectorAll('.timeframe-btn').forEach(btn => {
                    btn.addEventListener('click', function() {
                        document.querySelectorAll('.timeframe-btn').forEach(b => b.classList.remove('active'));
                        this.classList.add('active');
                        renderPriceChartCanvas(data.prices, this.dataset.range);
                    });
                });
            }, 100);
        } else {
            detailBody.innerHTML = '<div class="detail-loading">Keine Kursdaten verfügbar.</div>';
        }
    }

    function renderPriceChartCanvas(prices, range) {
        // Destroy existing chart
        if (priceChart) {
            priceChart.destroy();
            priceChart = null;
        }

        const canvas = document.getElementById('price-chart-canvas');
        if (!canvas) return;

        // Daten nach Zeitraum filtern
        let filteredPrices = prices;
        const now = new Date();
        let startDate;

        switch (range) {
            case '1m':
                startDate = new Date(now.setMonth(now.getMonth() - 1));
                break;
            case '3m':
                startDate = new Date(now.setMonth(now.getMonth() - 3));
                break;
            case '6m':
                startDate = new Date(now.setMonth(now.getMonth() - 6));
                break;
            case '1y':
                startDate = new Date(now.setFullYear(now.getFullYear() - 1));
                break;
            case '3y':
                startDate = new Date(now.setFullYear(now.getFullYear() - 3));
                break;
            case '5y':
                startDate = new Date(now.setFullYear(now.getFullYear() - 5));
                break;
            default:
                startDate = null; // all
        }

        if (startDate) {
            const startStr = startDate.toISOString().split('T')[0];
            filteredPrices = prices.filter(p => p.date >= startStr);
        }

        // Daten für Chart
        const labels = filteredPrices.map(p => {
            const d = new Date(p.date);
            return d.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', year: '2-digit' });
        });
        const dataPoints = filteredPrices.map(p => p.close);

        // Farbe basierend auf Performance
        const startPrice = dataPoints[0];
        const endPrice = dataPoints[dataPoints.length - 1];
        const isPositive = endPrice >= startPrice;
        const lineColor = isPositive ? '#28a745' : '#dc3545';
        const fillColor = isPositive ? 'rgba(40, 167, 69, 0.1)' : 'rgba(220, 53, 69, 0.1)';

        priceChart = new Chart(canvas, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Kurs',
                    data: dataPoints,
                    borderColor: lineColor,
                    backgroundColor: fillColor,
                    borderWidth: 2,
                    fill: true,
                    tension: 0.1,
                    pointRadius: 0,
                    pointHoverRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: (items) => {
                                return items[0]?.label || '';
                            },
                            label: (context) => {
                                const value = context.parsed.y;
                                return value?.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) || '-';
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        display: true,
                        ticks: {
                            maxTicksLimit: 8,
                            font: { size: 10 }
                        },
                        grid: { display: false }
                    },
                    y: {
                        display: true,
                        position: 'right',
                        ticks: {
                            font: { size: 10 },
                            callback: (value) => value.toLocaleString('de-DE', { minimumFractionDigits: 0 })
                        },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    // =========================================================================
    // Earnings Tab (jetzt im Unified Modal)
    // =========================================================================
    function renderEarningsTab(data) {
        // Header aktualisieren
        setModalHeader(data.company);

        // Formatierungsfunktionen
        const formatNumber = (val, decimals = 2) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
        };

        const formatBillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e9).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mrd.';
        };

        const formatMillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e6).toLocaleString('de-DE', { minimumFractionDigits: 0, maximumFractionDigits: 0 }) + ' Mio.';
        };

        const formatPercent = (val, showSign = false) => {
            if (val === null || val === undefined) return '-';
            const sign = showSign && val > 0 ? '+' : '';
            return sign + val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%';
        };

        const formatDate = (dateStr) => {
            if (!dateStr) return '-';
            const d = new Date(dateStr);
            return d.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', year: 'numeric' });
        };

        const getStatusIcon = (status) => {
            if (status === 'beat') return '<span class="earnings-status-beat">Beat</span>';
            if (status === 'miss') return '<span class="earnings-status-miss">Miss</span>';
            if (status === 'inline') return '<span class="earnings-status-inline">Inline</span>';
            return '-';
        };

        const getPriceReactionClass = (val) => {
            if (val === null || val === undefined) return '';
            return val >= 0 ? 'earnings-reaction-positive' : 'earnings-reaction-negative';
        };

        // 1. Hero-Section: Nächstes Quartal + Letztes Quartal kompakt nebeneinander
        let heroHtml = '<div class="earnings-hero-grid">';

        // Nächstes Quartal (links)
        if (data.next_earnings) {
            const ne = data.next_earnings;
            heroHtml += `
                <div class="earnings-hero earnings-hero-next">
                    <div class="earnings-hero-header">
                        <span class="earnings-hero-label-top">Nächstes Quartal</span>
                    </div>
                    <div class="earnings-hero-main">
                        <span class="earnings-hero-period">${ne.period || '-'}</span>
                        <span class="earnings-hero-date">${formatDate(ne.date)}</span>
                    </div>
                    <div class="earnings-hero-data">
                        <div class="earnings-hero-row">
                            <span class="earnings-hero-label">EPS Schätzung</span>
                            <span class="earnings-hero-value">${formatNumber(ne.eps_estimate)} ${data.company.currency || ''}</span>
                        </div>
                    </div>
                </div>
            `;
        } else {
            heroHtml += `
                <div class="earnings-hero earnings-hero-next earnings-hero-empty">
                    <p class="earnings-hero-no-data">Kein Termin</p>
                </div>
            `;
        }

        // Letztes Quartal (rechts)
        if (data.last_earnings) {
            const le = data.last_earnings;
            const surpriseClass = le.eps_surprise > 0 ? 'surprise-positive' : (le.eps_surprise < 0 ? 'surprise-negative' : '');
            heroHtml += `
                <div class="earnings-hero earnings-hero-last">
                    <div class="earnings-hero-header">
                        <span class="earnings-hero-label-top">Letztes Quartal</span>
                        ${le.status ? getStatusIcon(le.status) : ''}
                    </div>
                    <div class="earnings-hero-main">
                        <span class="earnings-hero-period">${le.period || '-'}</span>
                        <span class="earnings-hero-date">${formatDate(le.date)}</span>
                    </div>
                    <div class="earnings-hero-data">
                        <div class="earnings-hero-row">
                            <span class="earnings-hero-label">Schätzung</span>
                            <span class="earnings-hero-value">${formatNumber(le.eps_estimate)}</span>
                        </div>
                        <div class="earnings-hero-row">
                            <span class="earnings-hero-label">Ergebnis</span>
                            <span class="earnings-hero-value">${formatNumber(le.eps_actual)}</span>
                        </div>
                        <div class="earnings-hero-row">
                            <span class="earnings-hero-label">Abweichung</span>
                            <span class="earnings-hero-value ${surpriseClass}">${formatPercent(le.eps_surprise, true)}</span>
                        </div>
                    </div>
                </div>
            `;
        } else {
            heroHtml += `
                <div class="earnings-hero earnings-hero-last earnings-hero-empty">
                    <p class="earnings-hero-no-data">Keine Daten</p>
                </div>
            `;
        }

        heroHtml += '</div>';

        // 2. Fiskaljahr-Fortschritt (mit EPS + Revenue Tabelle)
        let fyProgressHtml = '';
        if (data.fiscal_year_progress && data.fiscal_year_progress.quarters && data.fiscal_year_progress.quarters.length > 0) {
            const fyp = data.fiscal_year_progress;
            const currency = data.company.currency || '';
            const periodLabel = fyp.period_label || 'Quartale';

            // Smart Revenue Formatter: automatisch Mrd/Mio
            const fmtRevenue = (val) => {
                if (val == null) return '-';
                const abs = Math.abs(val);
                if (abs >= 1e9) return (val / 1e9).toFixed(1) + ' Mrd';
                if (abs >= 1e6) return (val / 1e6).toFixed(0) + ' Mio';
                return formatNumber(val);
            };

            // Spalten-Header + Klassen pro Quartal
            const colHeaders = fyp.quarters.map(q => {
                const suffix = q.is_reported ? '' : (q.eps_source === 'implied' ? '*' : ' (e)');
                return `<th class="${q.is_reported ? 'fy-col-reported' : 'fy-col-estimated'}">${q.period_label}${suffix}</th>`;
            }).join('');

            // EPS-Zeile
            const epsValues = fyp.quarters.map(q => {
                const cls = q.is_reported ? 'fy-col-reported' : 'fy-col-estimated';
                const tooltip = q.eps_source === 'implied' ? ' title="Abgeleitet aus Jahresschätzung"' : '';
                const val = q.eps != null ? formatNumber(q.eps) : '-';
                return `<td class="${cls}"${tooltip}>${val}</td>`;
            }).join('');

            // Revenue-Zeile
            const revValues = fyp.quarters.map(q => {
                const cls = q.is_reported ? 'fy-col-reported' : 'fy-col-estimated';
                const tooltip = q.revenue_source === 'implied' ? ' title="Abgeleitet aus Jahresschätzung"' : '';
                const val = fmtRevenue(q.revenue);
                return `<td class="${cls}"${tooltip}>${val}</td>`;
            }).join('');

            fyProgressHtml = `
                <div class="earnings-section earnings-fy-progress">
                    <div class="fy-progress-header">
                        <span class="earnings-section-title">${fyp.year} Hochrechnung</span>
                        <span class="fy-progress-badge">${fyp.quarters_reported}/${fyp.total_quarters} ${periodLabel} gemeldet</span>
                    </div>
                    <div class="fy-progress-content">
                        <div class="fy-progress-table-container">
                            <table class="fy-progress-table">
                                <thead>
                                    <tr>
                                        <th></th>
                                        ${colHeaders}
                                        <th class="fy-col-sum">Summe</th>
                                        <th class="fy-col-annual">Jahres-Est.</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td class="fy-row-label">EPS</td>
                                        ${epsValues}
                                        <td class="fy-col-sum">${formatNumber(fyp.eps_sum)}</td>
                                        <td class="fy-col-annual">${fyp.annual_eps_estimate != null ? formatNumber(fyp.annual_eps_estimate) : '-'}</td>
                                    </tr>
                                    <tr>
                                        <td class="fy-row-label">Umsatz</td>
                                        ${revValues}
                                        <td class="fy-col-sum">${fmtRevenue(fyp.revenue_sum)}</td>
                                        <td class="fy-col-annual">${fyp.annual_revenue_estimate != null ? fmtRevenue(fyp.annual_revenue_estimate) : '-'}</td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                        <div class="fy-progress-bar-container">
                            <div class="fy-progress-bar" style="width: ${fyp.progress_pct}%"></div>
                        </div>
                        <div class="fy-progress-info">
                            <span class="fy-progress-text">${fyp.progress_pct}% des Fiskaljahres gemeldet</span>
                        </div>
                    </div>
                </div>
            `;
        }

        // 3. Quartals-Historie mit Beat/Miss
        let historyHtml = '';
        if (data.history && data.history.length > 0) {
            const historyRows = data.history.map(h => {
                const epsSurpriseClass = h.eps_surprise_pct > 0 ? 'surprise-positive' : (h.eps_surprise_pct < 0 ? 'surprise-negative' : '');
                const revSurpriseClass = h.revenue_surprise_pct > 0 ? 'surprise-positive' : (h.revenue_surprise_pct < 0 ? 'surprise-negative' : '');

                return `
                    <tr>
                        <td class="earnings-history-period">${h.period}</td>
                        <td class="earnings-history-date">${formatDate(h.release_date)}</td>
                        <td class="num">${formatNumber(h.eps_estimate)}</td>
                        <td class="num">${formatNumber(h.eps_actual)}</td>
                        <td class="num ${epsSurpriseClass}">${formatPercent(h.eps_surprise_pct, true)}</td>
                        <td class="num">${formatMillions(h.revenue_estimate)}</td>
                        <td class="num">${formatMillions(h.revenue_actual)}</td>
                        <td class="num ${revSurpriseClass}">${formatPercent(h.revenue_surprise_pct, true)}</td>
                        <td class="num ${getPriceReactionClass(h.price_reaction)}">${formatPercent(h.price_reaction, true)}</td>
                        <td class="earnings-status-cell">${getStatusIcon(h.status)}</td>
                    </tr>
                `;
            }).join('');

            historyHtml = `
                <div class="earnings-section">
                    <div class="earnings-section-title">Quartals-Historie (letzte 8 Quartale)</div>
                    <div class="earnings-history-table-container">
                        <table class="earnings-history-table">
                            <thead>
                                <tr>
                                    <th>Quartal</th>
                                    <th>Datum</th>
                                    <th class="num">EPS Est</th>
                                    <th class="num">EPS Act</th>
                                    <th class="num">Surprise</th>
                                    <th class="num">Rev Est</th>
                                    <th class="num">Rev Act</th>
                                    <th class="num">Surprise</th>
                                    <th class="num">Kursreaktion</th>
                                    <th>Status</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${historyRows}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        }

        // 4. Zukünftige Schätzungen
        let futureHtml = '';
        if (data.future_estimates && data.future_estimates.length > 0) {
            const futureRows = data.future_estimates.map(fe => `
                <tr>
                    <td class="earnings-future-period">${fe.period}</td>
                    <td class="num">${formatBillions(fe.revenue)}</td>
                    <td class="num">${formatBillions(fe.ebit)}</td>
                    <td class="num">${formatNumber(fe.eps)}</td>
                </tr>
            `).join('');

            futureHtml = `
                <div class="earnings-section">
                    <div class="earnings-section-title">Künftige Schätzungen</div>
                    <table class="earnings-future-table">
                        <thead>
                            <tr>
                                <th>Periode</th>
                                <th class="num">Umsatz</th>
                                <th class="num">EBIT</th>
                                <th class="num">EPS</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${futureRows}
                        </tbody>
                    </table>
                </div>
            `;
        }

        // Gesamtes HTML zusammenbauen
        let html = `
            <div class="earnings-tab-content">
                ${heroHtml}
                ${fyProgressHtml}
                ${historyHtml}
                ${futureHtml}
                ${(!data.history || data.history.length === 0) && (!data.future_estimates || data.future_estimates.length === 0) ?
                    '<div class="earnings-empty-state"><p>Keine Earnings-Daten verfügbar.</p></div>' : ''}
            </div>
        `;

        detailBody.innerHTML = html;
    }

    // Event-Listener für Klicks auf Unternehmensnamen
    function initializeCompanyNameClicks() {
        document.querySelectorAll('.stock-table tbody td.name').forEach(cell => {
            // Prüfen, ob bereits initialisiert
            if (cell.classList.contains('clickable-cell')) {
                return;
            }

            cell.classList.add('clickable-cell');
            cell.addEventListener('click', function(e) {
                const row = this.closest('tr');
                const isin = row?.dataset.isin;
                if (isin) {
                    openUnifiedModal(isin, 'info');
                }
            });
        });
    }

    // Initial aufrufen
    initializeCompanyNameClicks();

    // =========================================================================
    // DCF (Discounted Cash Flow) Modal
    // =========================================================================
    let dcfData = null;
    let dcfChart = null;
    let currentDcfResult = null;
    let currentScenarioId = null;

    async function loadDcfData(isin) {
        detailBody.innerHTML = '<div class="detail-loading">Lade DCF-Daten...</div>';

        try {
            const response = await fetch(`/api/stock/${isin}/dcf-data`);
            if (!response.ok) throw new Error('Fehler beim Laden');

            dcfData = await response.json();
            modalState.cache.dcfData = dcfData;
            currentScenarioId = null;
            currentDcfResult = null;
            renderDcfDetail(dcfData);

        } catch (error) {
            console.error('Fehler:', error);
            detailBody.innerHTML = '<div class="detail-loading">Fehler beim Laden der DCF-Daten.</div>';
        }
    }

    function renderDcfDetail(data) {
        // Header aktualisieren
        setModalHeader(data.company);

        const formatBillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e9).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mrd';
        };

        const formatNumber = (val, decimals = 1) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
        };

        const formatPercent = (val) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%';
        };

        const formatEps = (val) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        };

        // YoY-Berechnung: Prozentuale Veränderung zum Vorjahr
        const allData = [...data.historical, ...(data.estimates || [])];
        const calcYoy = (key) => {
            return allData.map((item, i) => {
                if (i === 0) return null;
                const prev = allData[i - 1][key];
                const curr = item[key];
                if (prev === null || prev === undefined || curr === null || curr === undefined || prev === 0) return null;
                return ((curr - prev) / Math.abs(prev)) * 100;
            });
        };
        // Für Margen: Differenz in Prozentpunkten statt prozentuale Veränderung
        const calcYoyMargin = (key) => {
            return allData.map((item, i) => {
                if (i === 0) return null;
                const prev = allData[i - 1][key];
                const curr = item[key];
                if (prev === null || prev === undefined || curr === null || curr === undefined) return null;
                return curr - prev;
            });
        };

        const yoyRevenue = calcYoy('revenue');
        const yoyEbit = calcYoy('ebit');
        const yoyFcf = calcYoy('fcf');
        const yoyNetIncome = calcYoy('net_income');
        const yoyEps = calcYoy('eps');
        const yoyEbitMargin = calcYoyMargin('ebit_margin');
        const yoyProfitMargin = calcYoyMargin('profit_margin');

        const numHistorical = data.historical.length;
        const formatYoy = (val) => {
            if (val === null || val === undefined) return '';
            const sign = val >= 0 ? '+' : '';
            return sign + val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%';
        };
        const formatYoyPp = (val) => {
            if (val === null || val === undefined) return '';
            const sign = val >= 0 ? '+' : '';
            return sign + val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + ' pp';
        };

        const buildYoyRow = (label, yoyArr, formatter) => {
            let cells = '';
            for (let i = 0; i < yoyArr.length; i++) {
                const cls = i >= numHistorical ? 'dcf-estimate-col' : 'dcf-historical-col';
                cells += `<td class="${cls}">${formatter(yoyArr[i])}</td>`;
            }
            return `<tr class="dcf-yoy-row"><td>${label}</td>${cells}</tr>`;
        };

        // Defaults oder geladenes Szenario verwenden
        const defaults = data.defaults;

        // Szenarien-Dropdown
        let scenarioOptions = '<option value="">Neues Szenario</option>';
        if (data.scenarios && data.scenarios.length > 0) {
            data.scenarios.forEach(s => {
                scenarioOptions += `<option value="${s.id}">${s.scenario_name} (${s.updated_at})</option>`;
            });
        }

        // Estimates vorbereiten
        const estimates = data.estimates || [];

        // HTML aufbauen
        let html = `
            <!-- Historische Daten + Estimates -->
            <div class="detail-section">
                <div class="detail-section-title">Historische Daten & Analystenschätzungen</div>
                <div class="dcf-data-table-container">
                    <table class="income-table dcf-full-width-table">
                        <thead>
                            <tr>
                                <th>Jahr</th>
                                ${data.historical.map(h => `<th class="dcf-historical-col">'${String(h.year).slice(-2)}</th>`).join('')}
                                ${estimates.map(e => `<th class="dcf-estimate-col">'${String(e.year).slice(-2)}e</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>Revenue</td>
                                ${data.historical.map(h => `<td class="dcf-historical-col">${formatBillions(h.revenue)}</td>`).join('')}
                                ${estimates.map(e => `<td class="dcf-estimate-col">${formatBillions(e.revenue)}</td>`).join('')}
                            </tr>
                            ${buildYoyRow('YoY', yoyRevenue, formatYoy)}
                            <tr>
                                <td>EBIT</td>
                                ${data.historical.map(h => `<td class="dcf-historical-col">${formatBillions(h.ebit)}</td>`).join('')}
                                ${estimates.map(e => `<td class="dcf-estimate-col">${formatBillions(e.ebit)}</td>`).join('')}
                            </tr>
                            ${buildYoyRow('YoY', yoyEbit, formatYoy)}
                            <tr>
                                <td>FCF</td>
                                ${data.historical.map(h => `<td class="dcf-historical-col">${formatBillions(h.fcf)}</td>`).join('')}
                                ${estimates.map(e => `<td class="dcf-estimate-col">${formatBillions(e.fcf)}</td>`).join('')}
                            </tr>
                            ${buildYoyRow('YoY', yoyFcf, formatYoy)}
                            <tr>
                                <td>Gewinn</td>
                                ${data.historical.map(h => `<td class="dcf-historical-col">${formatBillions(h.net_income)}</td>`).join('')}
                                ${estimates.map(e => `<td class="dcf-estimate-col">${formatBillions(e.net_income)}</td>`).join('')}
                            </tr>
                            ${buildYoyRow('YoY', yoyNetIncome, formatYoy)}
                            <tr>
                                <td>Gewinn/Aktie</td>
                                ${data.historical.map(h => `<td class="dcf-historical-col">${formatEps(h.eps)}</td>`).join('')}
                                ${estimates.map(e => `<td class="dcf-estimate-col">${formatEps(e.eps)}</td>`).join('')}
                            </tr>
                            <tr>
                                <td>EBIT-Marge</td>
                                ${data.historical.map(h => `<td class="dcf-historical-col">${formatPercent(h.ebit_margin)}</td>`).join('')}
                                ${estimates.map(e => `<td class="dcf-estimate-col">${formatPercent(e.ebit_margin)}</td>`).join('')}
                            </tr>
                            <tr>
                                <td>Gewinnmarge</td>
                                ${data.historical.map(h => `<td class="dcf-historical-col">${formatPercent(h.profit_margin)}</td>`).join('')}
                                ${estimates.map(e => `<td class="dcf-estimate-col">${formatPercent(e.profit_margin)}</td>`).join('')}
                            </tr>
                        </tbody>
                    </table>
                </div>
                <div class="dcf-cagr-row">
                    <div class="dcf-cagr-item">
                        <span class="dcf-cagr-label">CAGR 3J</span>
                        <span class="dcf-cagr-value">${formatPercent(data.cagr.cagr_3y)}</span>
                    </div>
                    <div class="dcf-cagr-item">
                        <span class="dcf-cagr-label">CAGR 5J</span>
                        <span class="dcf-cagr-value">${formatPercent(data.cagr.cagr_5y)}</span>
                    </div>
                    <div class="dcf-cagr-item">
                        <span class="dcf-cagr-label">CAGR 10J</span>
                        <span class="dcf-cagr-value">${formatPercent(data.cagr.cagr_10y)}</span>
                    </div>
                    <div class="dcf-legend">
                        <span class="dcf-legend-historical">Historisch</span>
                        <span class="dcf-legend-estimate">Schätzung (finanzen.net)</span>
                    </div>
                </div>
            </div>

            <!-- DCF Annahmen -->
            <div class="detail-section">
                <div class="detail-section-title dcf-assumptions-header">
                    DCF Annahmen
                    <select id="dcf-scenario-select" class="dcf-scenario-select">
                        ${scenarioOptions}
                    </select>
                </div>
                <div class="dcf-assumptions-grid">
                    <div class="dcf-input-group">
                        <label>Umsatzwachstum pro Jahr (%)</label>
                        <div class="dcf-growth-inputs">
                            <input type="number" id="dcf-growth-y1" step="0.1" value="${defaults.revenue_growth_y1}" placeholder="J1">
                            <input type="number" id="dcf-growth-y2" step="0.1" value="${defaults.revenue_growth_y2}" placeholder="J2">
                            <input type="number" id="dcf-growth-y3" step="0.1" value="${defaults.revenue_growth_y3}" placeholder="J3">
                            <input type="number" id="dcf-growth-y4" step="0.1" value="${defaults.revenue_growth_y4}" placeholder="J4">
                            <input type="number" id="dcf-growth-y5" step="0.1" value="${defaults.revenue_growth_y5}" placeholder="J5">
                            <input type="number" id="dcf-growth-y6" step="0.1" value="${defaults.revenue_growth_y6}" placeholder="J6">
                            <input type="number" id="dcf-growth-y7" step="0.1" value="${defaults.revenue_growth_y7}" placeholder="J7">
                            <input type="number" id="dcf-growth-y8" step="0.1" value="${defaults.revenue_growth_y8}" placeholder="J8">
                            <input type="number" id="dcf-growth-y9" step="0.1" value="${defaults.revenue_growth_y9}" placeholder="J9">
                            <input type="number" id="dcf-growth-y10" step="0.1" value="${defaults.revenue_growth_y10}" placeholder="J10">
                        </div>
                    </div>
                    <div class="dcf-input-group">
                        <label>EBIT-Marge pro Jahr (%)</label>
                        <div class="dcf-growth-inputs">
                            <input type="number" id="dcf-margin-y1" step="0.1" value="${defaults.ebit_margin_y1}" placeholder="J1">
                            <input type="number" id="dcf-margin-y2" step="0.1" value="${defaults.ebit_margin_y2}" placeholder="J2">
                            <input type="number" id="dcf-margin-y3" step="0.1" value="${defaults.ebit_margin_y3}" placeholder="J3">
                            <input type="number" id="dcf-margin-y4" step="0.1" value="${defaults.ebit_margin_y4}" placeholder="J4">
                            <input type="number" id="dcf-margin-y5" step="0.1" value="${defaults.ebit_margin_y5}" placeholder="J5">
                            <input type="number" id="dcf-margin-y6" step="0.1" value="${defaults.ebit_margin_y6}" placeholder="J6">
                            <input type="number" id="dcf-margin-y7" step="0.1" value="${defaults.ebit_margin_y7}" placeholder="J7">
                            <input type="number" id="dcf-margin-y8" step="0.1" value="${defaults.ebit_margin_y8}" placeholder="J8">
                            <input type="number" id="dcf-margin-y9" step="0.1" value="${defaults.ebit_margin_y9}" placeholder="J9">
                            <input type="number" id="dcf-margin-y10" step="0.1" value="${defaults.ebit_margin_y10}" placeholder="J10">
                        </div>
                    </div>
                    <div class="dcf-params-grid">
                        <div class="dcf-input-item">
                            <label>Steuersatz (%)</label>
                            <input type="number" id="dcf-tax-rate" step="0.1" value="${defaults.tax_rate}">
                        </div>
                        <div class="dcf-input-item">
                            <label>CapEx (%)</label>
                            <input type="number" id="dcf-capex" step="0.1" value="${defaults.capex_percent}">
                        </div>
                        <div class="dcf-input-item">
                            <label>D&A (%)</label>
                            <input type="number" id="dcf-depreciation" step="0.1" value="${defaults.depreciation_percent}">
                        </div>
                        <div class="dcf-input-item">
                            <label>WC-Änderung (%)</label>
                            <input type="number" id="dcf-wc-change" step="0.1" value="${defaults.wc_change_percent}">
                        </div>
                        <div class="dcf-input-item">
                            <label>Terminal Growth (%)</label>
                            <input type="number" id="dcf-terminal-growth" step="0.1" value="${defaults.terminal_growth}">
                        </div>
                        <div class="dcf-input-item">
                            <label>WACC (%)</label>
                            <input type="number" id="dcf-wacc" step="0.1" value="${defaults.wacc}">
                        </div>
                    </div>
                </div>

                <!-- WACC-Berechnung (CAPM) -->
                <div class="wacc-capm-section">
                    <div class="wacc-capm-title">WACC-Berechnung (CAPM)</div>

                    <div class="wacc-capm-subtitle">Eigenkapitalkosten (CAPM)</div>
                    <div class="wacc-capm-grid">
                        <div class="wacc-capm-item">
                            <label>Risikoloser Zins Rf (%)</label>
                            <input type="number" id="wacc-rf" step="0.1" value="${defaults.risk_free_rate}">
                        </div>
                        <div class="wacc-capm-item">
                            <label>Beta</label>
                            <input type="number" id="wacc-beta" step="0.01" value="${data.current.beta != null ? data.current.beta.toFixed(2) : '1.00'}">
                            ${data.current.beta == null ? '<span class="wacc-hint">kein Beta verfügbar, Default 1.0</span>' : ''}
                        </div>
                        <div class="wacc-capm-item">
                            <label>Markterwartung Rm (%)</label>
                            <input type="number" id="wacc-rm" step="0.1" value="${defaults.market_return}">
                        </div>
                        <div class="wacc-capm-item">
                            <label>EK-Kosten Re</label>
                            <span id="wacc-re-display" class="wacc-computed-value">-</span>
                        </div>
                    </div>

                    <div class="wacc-divider"></div>

                    <div class="wacc-capm-subtitle">Kapitalstruktur & WACC</div>
                    <div class="wacc-capm-grid wacc-capm-grid-2col">
                        <div class="wacc-capm-item">
                            <label>EK-Kosten Re</label>
                            <span id="wacc-re-display2" class="wacc-computed-value">-</span>
                        </div>
                        <div class="wacc-capm-item">
                            <label>FK-Zins Rd (%)</label>
                            <input type="number" id="wacc-rd" step="0.1" value="${defaults.debt_cost}">
                        </div>
                        <div class="wacc-capm-item">
                            <label>Market Cap (E)</label>
                            <span class="wacc-readonly-value">${data.current.market_cap ? formatBillions(data.current.market_cap) : '-'}</span>
                        </div>
                        <div class="wacc-capm-item">
                            <label>Total Debt in Mrd (D)</label>
                            <input type="number" id="wacc-total-debt" step="0.1" value="${data.current.total_debt ? (data.current.total_debt / 1e9).toFixed(2) : '0'}">
                        </div>
                        <div class="wacc-capm-item">
                            <label>EK-Anteil (E/V)</label>
                            <span id="wacc-equity-ratio" class="wacc-computed-value">-</span>
                        </div>
                        <div class="wacc-capm-item">
                            <label>FK-Anteil (D/V)</label>
                            <span id="wacc-debt-ratio" class="wacc-computed-value">-</span>
                        </div>
                        <div></div>
                        <div class="wacc-capm-item">
                            <label>Tax Shield (%)</label>
                            <input type="number" id="wacc-tax-shield" step="0.1" value="${defaults.tax_shield}">
                        </div>
                    </div>

                    <div class="wacc-capm-result">
                        <span class="wacc-result-label">Berechneter WACC:</span>
                        <span id="wacc-capm-result-value" class="wacc-result-value">-</span>
                        <button id="wacc-apply-btn" class="btn btn-sm">Übernehmen</button>
                    </div>
                </div>

                <div class="dcf-actions">
                    <button id="dcf-calculate-btn" class="btn btn-primary">Berechnen</button>
                    <button id="dcf-save-btn" class="btn">Szenario speichern</button>
                    <input type="text" id="dcf-scenario-name" class="dcf-scenario-name-input" placeholder="Szenario-Name">
                </div>
            </div>

            <!-- Ergebnisse (initial leer) -->
            <div id="dcf-results-container"></div>
        `;

        detailBody.innerHTML = html;

        // Auto-Scroll: Tabelle nach rechts scrollen damit Estimates sichtbar sind
        const dcfTableContainer = document.querySelector('.dcf-data-table-container');
        if (dcfTableContainer) dcfTableContainer.scrollLeft = dcfTableContainer.scrollWidth;

        // Event-Listener für Berechnen-Button
        document.getElementById('dcf-calculate-btn').addEventListener('click', () => {
            calculateDcf(data.company.isin);
        });

        // Event-Listener für Speichern-Button
        document.getElementById('dcf-save-btn').addEventListener('click', () => {
            saveDcfScenario(data.company.isin);
        });

        // Event-Listener für Szenario-Dropdown
        document.getElementById('dcf-scenario-select').addEventListener('change', function() {
            const scenarioId = this.value;
            if (scenarioId) {
                loadDcfScenario(parseInt(scenarioId));
            } else {
                // Defaults laden
                document.getElementById('dcf-growth-y1').value = defaults.revenue_growth_y1;
                document.getElementById('dcf-growth-y2').value = defaults.revenue_growth_y2;
                document.getElementById('dcf-growth-y3').value = defaults.revenue_growth_y3;
                document.getElementById('dcf-growth-y4').value = defaults.revenue_growth_y4;
                document.getElementById('dcf-growth-y5').value = defaults.revenue_growth_y5;
                document.getElementById('dcf-growth-y6').value = defaults.revenue_growth_y6;
                document.getElementById('dcf-growth-y7').value = defaults.revenue_growth_y7;
                document.getElementById('dcf-growth-y8').value = defaults.revenue_growth_y8;
                document.getElementById('dcf-growth-y9').value = defaults.revenue_growth_y9;
                document.getElementById('dcf-growth-y10').value = defaults.revenue_growth_y10;
                document.getElementById('dcf-margin-y1').value = defaults.ebit_margin_y1;
                document.getElementById('dcf-margin-y2').value = defaults.ebit_margin_y2;
                document.getElementById('dcf-margin-y3').value = defaults.ebit_margin_y3;
                document.getElementById('dcf-margin-y4').value = defaults.ebit_margin_y4;
                document.getElementById('dcf-margin-y5').value = defaults.ebit_margin_y5;
                document.getElementById('dcf-margin-y6').value = defaults.ebit_margin_y6;
                document.getElementById('dcf-margin-y7').value = defaults.ebit_margin_y7;
                document.getElementById('dcf-margin-y8').value = defaults.ebit_margin_y8;
                document.getElementById('dcf-margin-y9').value = defaults.ebit_margin_y9;
                document.getElementById('dcf-margin-y10').value = defaults.ebit_margin_y10;
                document.getElementById('dcf-tax-rate').value = defaults.tax_rate;
                document.getElementById('dcf-capex').value = defaults.capex_percent;
                document.getElementById('dcf-depreciation').value = defaults.depreciation_percent;
                document.getElementById('dcf-wc-change').value = defaults.wc_change_percent;
                document.getElementById('dcf-terminal-growth').value = defaults.terminal_growth;
                document.getElementById('dcf-wacc').value = defaults.wacc;
                document.getElementById('dcf-scenario-name').value = '';
                currentScenarioId = null;
                document.getElementById('dcf-results-container').innerHTML = '';
            }
        });

        // WACC-CAPM Berechnung
        const marketCap = data.current.market_cap;

        function calculateWacc() {
            const rf = parseFloat(document.getElementById('wacc-rf').value) || 0;
            const beta = parseFloat(document.getElementById('wacc-beta').value) || 1;
            const rm = parseFloat(document.getElementById('wacc-rm').value) || 0;
            const rd = parseFloat(document.getElementById('wacc-rd').value) || 0;
            const taxShield = parseFloat(document.getElementById('wacc-tax-shield').value) || 0;
            const totalDebt = (parseFloat(document.getElementById('wacc-total-debt').value) || 0) * 1e9;

            // CAPM: Re = Rf + Beta * (Rm - Rf)
            const re = rf + beta * (rm - rf);
            document.getElementById('wacc-re-display').textContent = re.toFixed(2) + '%';
            document.getElementById('wacc-re-display2').textContent = re.toFixed(2) + '%';

            // Kapitalstruktur
            if (marketCap && totalDebt > 0) {
                const v = marketCap + totalDebt;
                const eRatio = (marketCap / v) * 100;
                const dRatio = (totalDebt / v) * 100;
                document.getElementById('wacc-equity-ratio').textContent = eRatio.toFixed(1) + '%';
                document.getElementById('wacc-debt-ratio').textContent = dRatio.toFixed(1) + '%';

                // WACC = (E/V) * Re + (D/V) * Rd * (1 - Tax)
                const wacc = (marketCap / v) * re + (totalDebt / v) * rd * (1 - taxShield / 100);
                document.getElementById('wacc-capm-result-value').textContent = wacc.toFixed(2) + '%';
            } else {
                // Ohne Market Cap/Debt: nur Re anzeigen, WACC = Re als Fallback
                document.getElementById('wacc-equity-ratio').textContent = '-';
                document.getElementById('wacc-debt-ratio').textContent = '-';
                document.getElementById('wacc-capm-result-value').textContent = re.toFixed(2) + '% (nur EK)';
            }
        }

        // Live-Berechnung bei jeder Eingabeänderung
        ['wacc-rf', 'wacc-beta', 'wacc-rm', 'wacc-rd', 'wacc-tax-shield', 'wacc-total-debt'].forEach(id => {
            document.getElementById(id).addEventListener('input', calculateWacc);
        });

        // Übernehmen-Button: WACC in das DCF-WACC-Feld setzen
        document.getElementById('wacc-apply-btn').addEventListener('click', () => {
            const resultText = document.getElementById('wacc-capm-result-value').textContent;
            const match = resultText.match(/([\d.]+)%/);
            if (match) {
                document.getElementById('dcf-wacc').value = parseFloat(match[1]).toFixed(1);
            }
        });

        // Initial berechnen
        calculateWacc();

        // FCF Chart rendern
        if (typeof Chart !== 'undefined' && data.historical.length > 0) {
            setTimeout(() => renderDcfHistoricalChart(data.historical), 100);
        }
    }

    function loadDcfScenario(scenarioId) {
        const scenario = dcfData.scenarios.find(s => s.id === scenarioId);
        if (!scenario) return;

        document.getElementById('dcf-growth-y1').value = scenario.revenue_growth_y1 || 0;
        document.getElementById('dcf-growth-y2').value = scenario.revenue_growth_y2 || 0;
        document.getElementById('dcf-growth-y3').value = scenario.revenue_growth_y3 || 0;
        document.getElementById('dcf-growth-y4').value = scenario.revenue_growth_y4 || 0;
        document.getElementById('dcf-growth-y5').value = scenario.revenue_growth_y5 || 0;
        document.getElementById('dcf-growth-y6').value = scenario.revenue_growth_y6 || 0;
        document.getElementById('dcf-growth-y7').value = scenario.revenue_growth_y7 || 0;
        document.getElementById('dcf-growth-y8').value = scenario.revenue_growth_y8 || 0;
        document.getElementById('dcf-growth-y9').value = scenario.revenue_growth_y9 || 0;
        document.getElementById('dcf-growth-y10').value = scenario.revenue_growth_y10 || 0;
        const dflt = dcfData.defaults || {};
        document.getElementById('dcf-margin-y1').value = scenario.ebit_margin_y1 || dflt.ebit_margin_y1 || 15;
        document.getElementById('dcf-margin-y2').value = scenario.ebit_margin_y2 || dflt.ebit_margin_y2 || 15;
        document.getElementById('dcf-margin-y3').value = scenario.ebit_margin_y3 || dflt.ebit_margin_y3 || 15;
        document.getElementById('dcf-margin-y4').value = scenario.ebit_margin_y4 || dflt.ebit_margin_y4 || 15;
        document.getElementById('dcf-margin-y5').value = scenario.ebit_margin_y5 || dflt.ebit_margin_y5 || 15;
        document.getElementById('dcf-margin-y6').value = scenario.ebit_margin_y6 || dflt.ebit_margin_y6 || 15;
        document.getElementById('dcf-margin-y7').value = scenario.ebit_margin_y7 || dflt.ebit_margin_y7 || 15;
        document.getElementById('dcf-margin-y8').value = scenario.ebit_margin_y8 || dflt.ebit_margin_y8 || 15;
        document.getElementById('dcf-margin-y9').value = scenario.ebit_margin_y9 || dflt.ebit_margin_y9 || 15;
        document.getElementById('dcf-margin-y10').value = scenario.ebit_margin_y10 || dflt.ebit_margin_y10 || 15;
        document.getElementById('dcf-tax-rate').value = scenario.tax_rate || 25;
        document.getElementById('dcf-capex').value = scenario.capex_percent || 3;
        document.getElementById('dcf-depreciation').value = scenario.depreciation_percent || 3;
        document.getElementById('dcf-wc-change').value = scenario.wc_change_percent || 0;
        document.getElementById('dcf-terminal-growth').value = scenario.terminal_growth || 2;
        document.getElementById('dcf-wacc').value = scenario.wacc || 9;
        document.getElementById('dcf-scenario-name').value = scenario.scenario_name;
        currentScenarioId = scenarioId;
    }

    async function calculateDcf(isin) {
        const assumptions = {
            revenue_growth_y1: parseFloat(document.getElementById('dcf-growth-y1').value) || 0,
            revenue_growth_y2: parseFloat(document.getElementById('dcf-growth-y2').value) || 0,
            revenue_growth_y3: parseFloat(document.getElementById('dcf-growth-y3').value) || 0,
            revenue_growth_y4: parseFloat(document.getElementById('dcf-growth-y4').value) || 0,
            revenue_growth_y5: parseFloat(document.getElementById('dcf-growth-y5').value) || 0,
            revenue_growth_y6: parseFloat(document.getElementById('dcf-growth-y6').value) || 0,
            revenue_growth_y7: parseFloat(document.getElementById('dcf-growth-y7').value) || 0,
            revenue_growth_y8: parseFloat(document.getElementById('dcf-growth-y8').value) || 0,
            revenue_growth_y9: parseFloat(document.getElementById('dcf-growth-y9').value) || 0,
            revenue_growth_y10: parseFloat(document.getElementById('dcf-growth-y10').value) || 0,
            ebit_margin_y1: parseFloat(document.getElementById('dcf-margin-y1').value) || 15,
            ebit_margin_y2: parseFloat(document.getElementById('dcf-margin-y2').value) || 15,
            ebit_margin_y3: parseFloat(document.getElementById('dcf-margin-y3').value) || 15,
            ebit_margin_y4: parseFloat(document.getElementById('dcf-margin-y4').value) || 15,
            ebit_margin_y5: parseFloat(document.getElementById('dcf-margin-y5').value) || 15,
            ebit_margin_y6: parseFloat(document.getElementById('dcf-margin-y6').value) || 15,
            ebit_margin_y7: parseFloat(document.getElementById('dcf-margin-y7').value) || 15,
            ebit_margin_y8: parseFloat(document.getElementById('dcf-margin-y8').value) || 15,
            ebit_margin_y9: parseFloat(document.getElementById('dcf-margin-y9').value) || 15,
            ebit_margin_y10: parseFloat(document.getElementById('dcf-margin-y10').value) || 15,
            tax_rate: parseFloat(document.getElementById('dcf-tax-rate').value) || 25,
            capex_percent: parseFloat(document.getElementById('dcf-capex').value) || 3,
            depreciation_percent: parseFloat(document.getElementById('dcf-depreciation').value) || 3,
            wc_change_percent: parseFloat(document.getElementById('dcf-wc-change').value) || 0,
            terminal_growth: parseFloat(document.getElementById('dcf-terminal-growth').value) || 2,
            wacc: parseFloat(document.getElementById('dcf-wacc').value) || 9
        };

        const resultsContainer = document.getElementById('dcf-results-container');
        resultsContainer.innerHTML = '<div class="detail-loading">Berechne...</div>';

        try {
            const response = await fetch(`/api/stock/${isin}/dcf-calculate`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(assumptions)
            });

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.error || 'Berechnungsfehler');
            }

            const result = await response.json();
            currentDcfResult = result;
            renderDcfResults(result, assumptions);

        } catch (error) {
            console.error('Fehler:', error);
            resultsContainer.innerHTML = `<div class="dcf-error">Fehler: ${error.message}</div>`;
        }
    }

    function renderDcfResults(result, assumptions) {
        const formatBillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e9).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mrd';
        };

        const formatCurrency = (val) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        };

        const formatPercent = (val, showSign = false) => {
            if (val === null || val === undefined) return '-';
            const sign = showSign && val >= 0 ? '+' : '';
            return sign + val.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%';
        };

        // Upside/Downside Klasse
        const upsideClass = result.upside >= 0 ? 'dcf-upside' : 'dcf-downside';

        // Sensitivitätstabelle
        const sens = result.sensitivity;
        let sensitivityHtml = `
            <table class="dcf-sensitivity-table">
                <thead>
                    <tr>
                        <th>WACC \\ TGR</th>
                        ${sens.terminal_growth_values.map(tg => `<th>${tg}%</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
                    ${sens.matrix.map(row => `
                        <tr>
                            <td class="dcf-sens-wacc">${row.wacc}%</td>
                            ${row.values.map(v => `<td>${v !== null ? formatCurrency(v) : '-'}</td>`).join('')}
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;

        let html = `
            <!-- Ergebnis-Box -->
            <div class="detail-section">
                <div class="detail-section-title">DCF Ergebnis</div>
                <div class="dcf-results-grid">
                    <div class="dcf-valuation-box">
                        <div class="dcf-valuation-main">
                            <div class="dcf-fair-value">
                                <span class="dcf-fv-label">Fair Value</span>
                                <span class="dcf-fv-value">${formatCurrency(result.fair_value)} ${dcfData.company.currency || 'EUR'}</span>
                            </div>
                            <div class="dcf-current-price">
                                <span class="dcf-cp-label">Aktueller Kurs</span>
                                <span class="dcf-cp-value">${formatCurrency(result.current_price)} ${dcfData.company.currency || 'EUR'}</span>
                            </div>
                            <div class="dcf-upside-box ${upsideClass}">
                                <span class="dcf-upside-label">Potential</span>
                                <span class="dcf-upside-value">${formatPercent(result.upside, true)}</span>
                            </div>
                        </div>
                        <div class="dcf-valuation-details">
                            <div class="dcf-val-item">
                                <span>Enterprise Value</span>
                                <span>${formatBillions(result.enterprise_value)}</span>
                            </div>
                            <div class="dcf-val-item">
                                <span>- Net Debt</span>
                                <span>${formatBillions(result.net_debt)}</span>
                            </div>
                            <div class="dcf-val-item">
                                <span>= Equity Value</span>
                                <span>${formatBillions(result.equity_value)}</span>
                            </div>
                            <div class="dcf-val-item">
                                <span>/ Aktien</span>
                                <span>${(result.shares_outstanding / 1e6).toLocaleString('de-DE', { maximumFractionDigits: 1 })} Mio.</span>
                            </div>
                        </div>
                    </div>
                    <div class="dcf-sensitivity-box">
                        <div class="dcf-sens-title">Sensitivitätsanalyse</div>
                        ${sensitivityHtml}
                    </div>
                </div>
            </div>

            <!-- Prognose-Tabelle -->
            <div class="detail-section">
                <div class="detail-section-title">10-Jahres-Prognose (${dcfData.company.currency || 'EUR'})</div>
                <div class="income-table-container">
                    <table class="income-table dcf-projection-table">
                        <thead>
                            <tr>
                                <th></th>
                                ${result.projections.map(p => `<th>Jahr ${p.year}</th>`).join('')}
                                <th>Terminal</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>Revenue</td>
                                ${result.projections.map(p => `<td>${formatBillions(p.revenue)}</td>`).join('')}
                                <td>-</td>
                            </tr>
                            <tr>
                                <td>EBIT</td>
                                ${result.projections.map(p => `<td>${formatBillions(p.ebit)}</td>`).join('')}
                                <td>-</td>
                            </tr>
                            <tr>
                                <td>NOPAT</td>
                                ${result.projections.map(p => `<td>${formatBillions(p.nopat)}</td>`).join('')}
                                <td>-</td>
                            </tr>
                            <tr>
                                <td>+ D&A</td>
                                ${result.projections.map(p => `<td>${formatBillions(p.depreciation)}</td>`).join('')}
                                <td>-</td>
                            </tr>
                            <tr>
                                <td>- CapEx</td>
                                ${result.projections.map(p => `<td>${formatBillions(p.capex)}</td>`).join('')}
                                <td>-</td>
                            </tr>
                            <tr>
                                <td>- WC-Chg</td>
                                ${result.projections.map(p => `<td>${formatBillions(p.wc_change)}</td>`).join('')}
                                <td>-</td>
                            </tr>
                            <tr class="dcf-fcf-row">
                                <td>= FCF</td>
                                ${result.projections.map(p => `<td>${formatBillions(p.fcf)}</td>`).join('')}
                                <td>${formatBillions(result.terminal_value)}</td>
                            </tr>
                            <tr>
                                <td>Diskontfaktor</td>
                                ${result.projections.map(p => `<td>${p.discount_factor.toFixed(4)}</td>`).join('')}
                                <td>${(1 / Math.pow(1 + assumptions.wacc / 100, 5)).toFixed(4)}</td>
                            </tr>
                            <tr class="dcf-pv-row">
                                <td>= Barwert</td>
                                ${result.projections.map(p => `<td>${formatBillions(p.pv_fcf)}</td>`).join('')}
                                <td>${formatBillions(result.terminal_pv)}</td>
                            </tr>
                        </tbody>
                        <tfoot>
                            <tr>
                                <td>Summe Barwerte</td>
                                <td colspan="5">${formatBillions(result.sum_pv_fcf)}</td>
                                <td>${formatBillions(result.terminal_pv)}</td>
                            </tr>
                        </tfoot>
                    </table>
                </div>
            </div>

            <!-- FCF Chart -->
            <div class="detail-section">
                <div class="detail-section-title">Free Cash Flow (historisch + projiziert)</div>
                <div class="chart-container dcf-chart-container">
                    <div class="chart-wrapper dcf-chart-wrapper">
                        <canvas id="dcf-fcf-chart"></canvas>
                    </div>
                </div>
            </div>
        `;

        document.getElementById('dcf-results-container').innerHTML = html;

        // Chart rendern
        if (typeof Chart !== 'undefined') {
            renderDcfFcfChart(dcfData.historical, result.projections);
        }
    }

    function renderDcfHistoricalChart(historical) {
        // Bereits im Results-Container gerendert
    }

    function renderDcfFcfChart(historical, projections) {
        const ctx = document.getElementById('dcf-fcf-chart');
        if (!ctx) return;

        if (dcfChart) {
            dcfChart.destroy();
        }

        // Historische FCF
        const histLabels = historical.map(h => h.year);
        const histData = historical.map(h => h.fcf ? h.fcf / 1e9 : 0);

        // Projizierte FCF
        const projLabels = projections.map(p => `+${p.year}`);
        const projData = projections.map(p => p.fcf / 1e9);

        const labels = [...histLabels, ...projLabels];
        const fcfData = [...histData, ...projData];

        // Farben: historisch vs. projiziert
        const colors = fcfData.map((val, idx) => {
            if (idx < histData.length) {
                return val >= 0 ? '#2d5aa3' : '#dc3545';
            }
            return val >= 0 ? 'rgba(45, 90, 163, 0.5)' : 'rgba(220, 53, 69, 0.5)';
        });

        const borderColors = fcfData.map((val, idx) => {
            if (idx < histData.length) {
                return val >= 0 ? '#2d5aa3' : '#dc3545';
            }
            return val >= 0 ? '#2d5aa3' : '#dc3545';
        });

        dcfChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'FCF (Mrd)',
                    data: fcfData,
                    backgroundColor: colors,
                    borderColor: borderColors,
                    borderWidth: 1,
                    borderRadius: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    annotation: {
                        annotations: {
                            line1: {
                                type: 'line',
                                xMin: histData.length - 0.5,
                                xMax: histData.length - 0.5,
                                borderColor: '#999',
                                borderWidth: 2,
                                borderDash: [5, 5]
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { font: { size: 10 } },
                        grid: { display: false }
                    },
                    y: {
                        ticks: { font: { size: 10 } },
                        grid: { color: '#eee' }
                    }
                }
            }
        });
    }

    async function saveDcfScenario(isin) {
        const scenarioName = document.getElementById('dcf-scenario-name').value || 'Standard';

        const data = {
            scenario_id: currentScenarioId,
            scenario_name: scenarioName,
            revenue_growth_y1: parseFloat(document.getElementById('dcf-growth-y1').value) || 0,
            revenue_growth_y2: parseFloat(document.getElementById('dcf-growth-y2').value) || 0,
            revenue_growth_y3: parseFloat(document.getElementById('dcf-growth-y3').value) || 0,
            revenue_growth_y4: parseFloat(document.getElementById('dcf-growth-y4').value) || 0,
            revenue_growth_y5: parseFloat(document.getElementById('dcf-growth-y5').value) || 0,
            revenue_growth_y6: parseFloat(document.getElementById('dcf-growth-y6').value) || 0,
            revenue_growth_y7: parseFloat(document.getElementById('dcf-growth-y7').value) || 0,
            revenue_growth_y8: parseFloat(document.getElementById('dcf-growth-y8').value) || 0,
            revenue_growth_y9: parseFloat(document.getElementById('dcf-growth-y9').value) || 0,
            revenue_growth_y10: parseFloat(document.getElementById('dcf-growth-y10').value) || 0,
            ebit_margin_y1: parseFloat(document.getElementById('dcf-margin-y1').value) || 15,
            ebit_margin_y2: parseFloat(document.getElementById('dcf-margin-y2').value) || 15,
            ebit_margin_y3: parseFloat(document.getElementById('dcf-margin-y3').value) || 15,
            ebit_margin_y4: parseFloat(document.getElementById('dcf-margin-y4').value) || 15,
            ebit_margin_y5: parseFloat(document.getElementById('dcf-margin-y5').value) || 15,
            ebit_margin_y6: parseFloat(document.getElementById('dcf-margin-y6').value) || 15,
            ebit_margin_y7: parseFloat(document.getElementById('dcf-margin-y7').value) || 15,
            ebit_margin_y8: parseFloat(document.getElementById('dcf-margin-y8').value) || 15,
            ebit_margin_y9: parseFloat(document.getElementById('dcf-margin-y9').value) || 15,
            ebit_margin_y10: parseFloat(document.getElementById('dcf-margin-y10').value) || 15,
            tax_rate: parseFloat(document.getElementById('dcf-tax-rate').value) || 25,
            capex_percent: parseFloat(document.getElementById('dcf-capex').value) || 3,
            wc_change_percent: parseFloat(document.getElementById('dcf-wc-change').value) || 0,
            depreciation_percent: parseFloat(document.getElementById('dcf-depreciation').value) || 3,
            terminal_growth: parseFloat(document.getElementById('dcf-terminal-growth').value) || 2,
            wacc: parseFloat(document.getElementById('dcf-wacc').value) || 9,
            fair_value_per_share: currentDcfResult ? currentDcfResult.fair_value : null
        };

        try {
            const response = await fetch(`/api/stock/${isin}/dcf-save`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.error || 'Speicherfehler');
            }

            const result = await response.json();
            currentScenarioId = result.scenario_id;

            // Erfolgsmeldung
            alert('Szenario gespeichert!');

            // Daten neu laden um Dropdown zu aktualisieren
            loadDcfData(isin);

        } catch (error) {
            console.error('Fehler:', error);
            alert('Fehler beim Speichern: ' + error.message);
        }
    }

});
