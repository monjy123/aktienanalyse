// =============================================================================
// Aktien-Tool JavaScript
// =============================================================================

// View-Name aus URL ermitteln
function getCurrentView() {
    const path = window.location.pathname;
    if (path.includes('watchlist')) return 'watchlist';
    if (path.includes('screener')) return 'screener';
    return 'watchlist';
}

document.addEventListener('DOMContentLoaded', function() {

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
    document.querySelectorAll('.modal-close').forEach(btn => {
        btn.addEventListener('click', function() {
            this.closest('.modal').classList.add('hidden');
        });
    });

    // Klick außerhalb Modal
    document.querySelectorAll('.modal').forEach(modal => {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                this.classList.add('hidden');
            }
        });
    });

    // Escape-Taste
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal').forEach(modal => {
                modal.classList.add('hidden');
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
                    <select id="filter-${field}" class="filter-select">
                        <option value="">Alle</option>
                        ${values.map(v => `<option value="${v}">${v}</option>`).join('')}
                    </select>
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

        // Gespeicherte Filter wiederherstellen
        restoreSavedFilters();
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
            const select = document.getElementById(`filter-${field}`);
            if (select && currentFilters[field]) {
                select.value = currentFilters[field];
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
            const select = document.getElementById(`filter-${field}`);
            if (select && select.value) {
                filters[field] = select.value;
            } else if (select) {
                delete filters[field];
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
                const select = document.getElementById(`filter-${field}`);
                if (select) select.value = '';
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
            const dataType = col.format_type === 'text' ? 'text' : 'number';
            html += `<th class="sortable ${numClass}" data-column="${col.column_key}" data-type="${dataType}">${col.display_name}</th>`;
        }
        html += '<th>Notizen</th></tr></thead><tbody>';

        for (const stock of data.stocks) {
            html += `<tr data-isin="${stock.isin}">`;
            let favOptions = `<option value="0" ${(!stock.favorite || stock.favorite == 0) ? 'selected' : ''}>-</option>`;
            for (let i = 1; i <= 9; i++) {
                favOptions += `<option value="${i}" ${stock.favorite == i ? 'selected' : ''}>${i}</option>`;
            }
            html += `<td data-column="favorite" data-value="${stock.favorite || 0}">
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
                    } else {
                        displayValue = value;
                    }
                }

                html += `<td class="${numClass} ${nameClass}" data-column="${col.column_key}" data-value="${value !== null && value !== undefined ? value : ''}">${displayValue}</td>`;
            }

            html += `<td>
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
    const detailTabs = document.querySelectorAll('.detail-tab');

    let peChart = null;
    let incomeChart = null;
    let currentDetailData = null; // Gecachte Daten für Tab-Wechsel
    let currentTab = 'pe';

    // Tab-Klicks
    detailTabs.forEach(tab => {
        tab.addEventListener('click', function() {
            const tabType = this.dataset.tab;
            if (tabType === currentTab || !currentDetailData) return;

            // Aktiven Tab wechseln
            detailTabs.forEach(t => t.classList.remove('active'));
            this.classList.add('active');
            currentTab = tabType;

            // Content rendern
            if (tabType === 'ev_ebit') {
                renderEvEbitDetail(currentDetailData);
            } else if (tabType === 'growth') {
                renderGrowthDetail(currentDetailData);
            } else if (tabType === 'margins') {
                renderMarginsDetail(currentDetailData);
            } else {
                renderStockDetail(currentDetailData);
            }
        });
    });

    // KGV-relevante Spalten, die das PE-Modal öffnen
    const PE_COLUMNS = [
        'ttm_pe', 'fy_pe',
        'pe_avg_5y', 'pe_avg_10y', 'pe_avg_15y', 'pe_avg_20y', 'pe_avg_10y_2019',
        'yf_ttm_pe', 'yf_forward_pe',
        'yf_ttm_pe_vs_avg_5y', 'yf_ttm_pe_vs_avg_10y', 'yf_ttm_pe_vs_avg_15y', 'yf_ttm_pe_vs_avg_20y', 'yf_ttm_pe_vs_avg_10y_2019',
        'yf_fwd_pe_vs_avg_5y', 'yf_fwd_pe_vs_avg_10y', 'yf_fwd_pe_vs_avg_15y', 'yf_fwd_pe_vs_avg_20y', 'yf_fwd_pe_vs_avg_10y_2019'
    ];

    // EV/EBIT-relevante Spalten
    const EV_EBIT_COLUMNS = [
        'ttm_ev_ebit', 'fy_ev_ebit',
        'ev_ebit_avg_5y', 'ev_ebit_avg_10y', 'ev_ebit_avg_15y', 'ev_ebit_avg_20y', 'ev_ebit_avg_10y_2019',
        'ev_ebit_vs_avg_5y', 'ev_ebit_vs_avg_10y', 'ev_ebit_vs_avg_15y', 'ev_ebit_vs_avg_20y', 'ev_ebit_vs_avg_10y_2019'
    ];

    // Wachstums-relevante Spalten
    const GROWTH_COLUMNS = [
        'revenue_cagr_3y', 'revenue_cagr_5y', 'revenue_cagr_10y',
        'ebit_cagr_3y', 'ebit_cagr_5y', 'ebit_cagr_10y',
        'net_income_cagr_3y', 'net_income_cagr_5y', 'net_income_cagr_10y'
    ];

    // Margen-relevante Spalten
    const MARGIN_COLUMNS = [
        'profit_margin', 'operating_margin',
        'profit_margin_avg_3y', 'profit_margin_avg_5y', 'profit_margin_avg_10y', 'profit_margin_avg_5y_2019',
        'operating_margin_avg_3y', 'operating_margin_avg_5y', 'operating_margin_avg_10y', 'operating_margin_avg_5y_2019'
    ];

    // Funktion zum Initialisieren der clickable cells
    function initializeClickableCells() {
        document.querySelectorAll('.stock-table tbody td[data-column]').forEach(cell => {
            const columnKey = cell.dataset.column;

            // company_name überspringen (wird separat behandelt)
            if (columnKey === 'company_name') {
                return;
            }

            // Entferne alte Event-Listener, indem wir die Klasse prüfen
            if (cell.classList.contains('clickable-cell')) {
                // Zelle wurde bereits initialisiert, überspringen
                return;
            }

            // KGV-Spalten
            if (PE_COLUMNS.includes(columnKey)) {
                cell.classList.add('clickable-cell');
                cell.addEventListener('click', function(e) {
                    const row = this.closest('tr');
                    const isin = row?.dataset.isin;
                    if (isin) {
                        openStockDetail(isin, 'pe');
                    }
                });
            }

            // EV/EBIT-Spalten
            else if (EV_EBIT_COLUMNS.includes(columnKey)) {
                cell.classList.add('clickable-cell');
                cell.addEventListener('click', function(e) {
                    const row = this.closest('tr');
                    const isin = row?.dataset.isin;
                    if (isin) {
                        openStockDetail(isin, 'ev_ebit');
                    }
                });
            }

            // Wachstums-Spalten
            else if (GROWTH_COLUMNS.includes(columnKey)) {
                cell.classList.add('clickable-cell');
                cell.addEventListener('click', function(e) {
                    const row = this.closest('tr');
                    const isin = row?.dataset.isin;
                    if (isin) {
                        openStockDetail(isin, 'growth');
                    }
                });
            }

            // Margen-Spalten
            else if (MARGIN_COLUMNS.includes(columnKey)) {
                cell.classList.add('clickable-cell');
                cell.addEventListener('click', function(e) {
                    const row = this.closest('tr');
                    const isin = row?.dataset.isin;
                    if (isin) {
                        openStockDetail(isin, 'margins');
                    }
                });
            }
        });
    }

    // Klick auf Tabellenzellen mit Kennzahlen initialisieren
    initializeClickableCells();

    async function openStockDetail(isin, type = 'pe') {
        if (!stockDetailModal) return;

        // Modal öffnen mit Ladeindikator
        stockDetailModal.classList.remove('hidden');
        detailCompanyName.textContent = 'Lade...';
        detailMeta.textContent = '';
        detailBody.innerHTML = '<div class="detail-loading">Lade Daten...</div>';

        // Aktiven Tab setzen
        currentTab = type;
        detailTabs.forEach(tab => {
            tab.classList.toggle('active', tab.dataset.tab === type);
        });

        try {
            const response = await fetch(`/api/stock/${isin}/details`);
            if (!response.ok) throw new Error('Fehler beim Laden');

            const data = await response.json();
            currentDetailData = data; // Daten cachen für Tab-Wechsel

            // Je nach Typ unterschiedliches Modal rendern
            if (type === 'ev_ebit') {
                renderEvEbitDetail(data);
            } else if (type === 'growth') {
                renderGrowthDetail(data);
            } else if (type === 'margins') {
                renderMarginsDetail(data);
            } else {
                renderStockDetail(data);
            }

        } catch (error) {
            console.error('Fehler:', error);
            detailBody.innerHTML = '<div class="detail-loading">Fehler beim Laden der Daten.</div>';
        }
    }

    function renderStockDetail(data) {
        // Header
        detailCompanyName.textContent = data.company.name || '-';
        const fiscalYear = data.company.fiscal_year_end ? ` | FJ: ${data.company.fiscal_year_end}` : '';
        detailMeta.textContent = `${data.company.ticker} | ${data.company.sector || '-'} | ${data.company.country || '-'}${fiscalYear}`;

        // Formatierungsfunktionen
        const formatBillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e9).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mrd';
        };

        const formatNumber = (val, decimals = 1) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
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
                        <td class="pe-value">${formatNumber(pe.pe_avg_5y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_ttm_pe_vs_avg_5y)}">${formatDiff(pe.yf_ttm_pe_vs_avg_5y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_fwd_pe_vs_avg_5y)}">${formatDiff(pe.yf_fwd_pe_vs_avg_5y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 10J</td>
                        <td class="pe-value">${formatNumber(pe.pe_avg_10y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_ttm_pe_vs_avg_10y)}">${formatDiff(pe.yf_ttm_pe_vs_avg_10y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_fwd_pe_vs_avg_10y)}">${formatDiff(pe.yf_fwd_pe_vs_avg_10y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 15J</td>
                        <td class="pe-value">${formatNumber(pe.pe_avg_15y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_ttm_pe_vs_avg_15y)}">${formatDiff(pe.yf_ttm_pe_vs_avg_15y)}</td>
                        <td class="pe-diff ${getDiffClass(pe.yf_fwd_pe_vs_avg_15y)}">${formatDiff(pe.yf_fwd_pe_vs_avg_15y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 20J</td>
                        <td class="pe-value">${formatNumber(pe.pe_avg_20y)}</td>
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
        }
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

        peChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'KGV',
                    data: peData,
                    borderColor: '#1a1a2e',
                    backgroundColor: 'rgba(26, 26, 46, 0.1)',
                    borderWidth: 2,
                    pointRadius: 3,
                    pointBackgroundColor: '#1a1a2e',
                    fill: true,
                    tension: 0.1
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
    let evEbitChart = null;
    let ebitChart = null;

    function renderEvEbitDetail(data) {
        // Header
        detailCompanyName.textContent = data.company.name || '-';
        const fiscalYear = data.company.fiscal_year_end ? ` | FJ: ${data.company.fiscal_year_end}` : '';
        detailMeta.textContent = `${data.company.ticker} | ${data.company.sector || '-'} | ${data.company.country || '-'}${fiscalYear}`;

        // Formatierungsfunktionen
        const formatBillions = (val) => {
            if (val === null || val === undefined) return '-';
            return (val / 1e9).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' Mrd';
        };

        const formatNumber = (val, decimals = 1) => {
            if (val === null || val === undefined) return '-';
            return val.toLocaleString('de-DE', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
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
                        <td class="pe-value">${formatNumber(evEbit.ev_ebit_avg_5y)}</td>
                        <td class="pe-diff ${getDiffClass(evEbit.ev_ebit_vs_avg_5y)}">${formatDiff(evEbit.ev_ebit_vs_avg_5y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 10J</td>
                        <td class="pe-value">${formatNumber(evEbit.ev_ebit_avg_10y)}</td>
                        <td class="pe-diff ${getDiffClass(evEbit.ev_ebit_vs_avg_10y)}">${formatDiff(evEbit.ev_ebit_vs_avg_10y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 15J</td>
                        <td class="pe-value">${formatNumber(evEbit.ev_ebit_avg_15y)}</td>
                        <td class="pe-diff ${getDiffClass(evEbit.ev_ebit_vs_avg_15y)}">${formatDiff(evEbit.ev_ebit_vs_avg_15y)}</td>
                    </tr>
                    <tr>
                        <td class="pe-label">Ø 20J</td>
                        <td class="pe-value">${formatNumber(evEbit.ev_ebit_avg_20y)}</td>
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

        evEbitChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'EV/EBIT',
                    data: evEbitData,
                    borderColor: '#2d5aa3',
                    backgroundColor: 'rgba(45, 90, 163, 0.1)',
                    borderWidth: 2,
                    pointRadius: 3,
                    pointBackgroundColor: '#2d5aa3',
                    fill: true,
                    tension: 0.1
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
    // Wachstum Detail Modal
    // =========================================================================
    let revenueChart = null;
    let netIncomeChart = null;

    function renderGrowthDetail(data) {
        // Header
        detailCompanyName.textContent = data.company.name || '-';
        const fiscalYear = data.company.fiscal_year_end ? ` | FJ: ${data.company.fiscal_year_end}` : '';
        detailMeta.textContent = `${data.company.ticker} | ${data.company.sector || '-'} | ${data.company.country || '-'}${fiscalYear}`;

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
        detailCompanyName.textContent = data.company.name || '-';
        const fiscalYear = data.company.fiscal_year_end ? ` | FJ: ${data.company.fiscal_year_end}` : '';
        detailMeta.textContent = `${data.company.ticker} | ${data.company.sector || '-'} | ${data.company.country || '-'}${fiscalYear}`;

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
    // Company Info Modal (Unternehmensbeschreibung)
    // =========================================================================
    const companyInfoModal = document.getElementById('company-info-modal');
    const infoCompanyName = document.getElementById('info-company-name');
    const infoMeta = document.getElementById('info-meta');
    const companyInfoBody = document.getElementById('info-body');

    async function openCompanyInfo(isin) {
        if (!companyInfoModal) return;

        // Modal öffnen mit Ladeindikator
        companyInfoModal.classList.remove('hidden');
        infoCompanyName.textContent = 'Lade...';
        infoMeta.textContent = '';
        companyInfoBody.innerHTML = '<div class="detail-loading">Lade Daten...</div>';

        try {
            const response = await fetch(`/api/stock/${isin}/info`);
            if (!response.ok) throw new Error('Fehler beim Laden');

            const data = await response.json();

            // Header
            infoCompanyName.textContent = data.company_name || '-';
            const fiscalYear = data.fiscal_year_end ? ` | FJ: ${data.fiscal_year_end}` : '';
            infoMeta.textContent = `${data.ticker} | ${data.sector || '-'} | ${data.country || '-'}${fiscalYear}`;

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
            html += '</div>';

            html += '</div>';
            companyInfoBody.innerHTML = html;

        } catch (error) {
            console.error('Fehler:', error);
            companyInfoBody.innerHTML = '<div class="detail-loading">Fehler beim Laden der Daten.</div>';
        }
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
                    openCompanyInfo(isin);
                }
            });
        });
    }

    // Initial aufrufen
    initializeCompanyNameClicks();

});
