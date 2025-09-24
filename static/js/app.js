// Keep variable names and comments in English.

async function postJSON(url, payload) {
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
  return resp.json();
}

function toggleClass(el, className, on) {
  if (!el) return;
  el.classList.toggle(className, on);
}

function isPastDateStr(iso) {
  if (!iso) return false;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const d = new Date(`${iso}T00:00:00`);
  return d < today;
}

const GERMANY_BOUNDS = {
  latMin: 47.2701114,
  latMax: 55.058347,
  lonMin: 5.8663153,
  lonMax: 15.0418962,
};

const latField = document.getElementById('address_lat');
const lonField = document.getElementById('address_lon');
const countryField = document.getElementById('address_country');
const latLabel = document.getElementById('coord_lat');
const lonLabel = document.getElementById('coord_lon');
const regionLabel = document.getElementById('coord_region');
const coordError = document.getElementById('coord_error');
const resultBody = document.querySelector('#result .result-body');

let activeMarker = null;
let leafletMap = null;
let startPicker = null;
let endPicker = null;

const REGION_LOADING_TEXT = "Ort wird geladen ...";
const REGION_DEFAULT_TEXT = "Deutschland";
const REGION_EMPTY_TEXT = "Noch keine Auswahl";
const THEME_STORAGE_KEY = "weather_theme_preference";

let regionLookupSeq = 0;

const MAX_LEAFLET_ATTEMPTS = 20;
const LEAFLET_RETRY_DELAY_MS = 150;
let leafletFallbackInjected = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function injectLeafletFallback() {
  if (leafletFallbackInjected) return;
  leafletFallbackInjected = true;

  const head = document.head || document.getElementsByTagName('head')[0];
  if (!head) return;

  const css = document.createElement('link');
  css.rel = 'stylesheet';
  css.href = 'https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css';
  css.integrity = 'sha256-o9N1j7kGStzbYVm++H2bHtMICc0545G7Vp3u0wH+0SY=';
  css.crossOrigin = '';
  head.appendChild(css);

  const script = document.createElement('script');
  script.src = 'https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js';
  script.defer = true;
  script.integrity = 'sha256-Vt3f72rT0G0GvZ5G8J3QAi6pY9dM651TU01M9y+4SU8=';
  script.crossOrigin = '';
  head.appendChild(script);
}

function isInGermany(lat, lon) {
  return lat >= GERMANY_BOUNDS.latMin && lat <= GERMANY_BOUNDS.latMax &&
    lon >= GERMANY_BOUNDS.lonMin && lon <= GERMANY_BOUNDS.lonMax;
}

function formatCoordinate(value, axis) {
  const direction = axis === 'lat'
    ? (value >= 0 ? 'N' : 'S')
    : (value >= 0 ? 'E' : 'W');
  return `${Math.abs(value).toFixed(4)}\u00B0 ${direction}`;
}



function setRegionLabel(text) {
  if (!regionLabel) return;
  regionLabel.textContent = text;
}

async function resolveRegionName(lat, lon) {
  if (!regionLabel) return;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

  regionLookupSeq += 1;
  const lookupId = regionLookupSeq;
  setRegionLabel(REGION_LOADING_TEXT);

  try {
    const resp = await fetch(`/api/reverse_geocode?lat=${lat}&lon=${lon}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    if (lookupId !== regionLookupSeq) return;

    const city = data.city || data.town || data.village || data.municipality || data.county || data.state;
    const label = city ? `${city}, ${REGION_DEFAULT_TEXT}` : REGION_DEFAULT_TEXT;
    setRegionLabel(label);
  } catch (err) {
    if (lookupId === regionLookupSeq) {
      setRegionLabel(REGION_DEFAULT_TEXT);
    }
    console.error('Failed to resolve region name', err);
  }
}

function updateCoordinateSummary(lat, lon) {
  const fixedLat = Number(lat.toFixed(6));
  const fixedLon = Number(lon.toFixed(6));
  latField.value = fixedLat;
  lonField.value = fixedLon;
  countryField.value = 'de';
  if (latLabel) latLabel.textContent = formatCoordinate(fixedLat, 'lat');
  if (lonLabel) lonLabel.textContent = formatCoordinate(fixedLon, 'lon');
  resolveRegionName(fixedLat, fixedLon);
}

function resetCoordinateSummary() {
  regionLookupSeq += 1;
  latField.value = '';
  lonField.value = '';
  countryField.value = '';
  if (latLabel) latLabel.textContent = '--';
  if (lonLabel) lonLabel.textContent = '--';
  setRegionLabel(REGION_EMPTY_TEXT);
}

function showCoordinateError(message) {
  if (!coordError) return;
  if (message) {
    coordError.textContent = message;
    toggleClass(coordError, 'hidden', false);
  } else {
    toggleClass(coordError, 'hidden', true);
  }
}

function markMapUnavailable(message) {
  const mapElement = document.getElementById('map');
  if (!mapElement) return;
  mapElement.innerHTML = '';
  const fallback = document.createElement('div');
  fallback.textContent = message;
  fallback.style.display = 'flex';
  fallback.style.alignItems = 'center';
  fallback.style.justifyContent = 'center';
  fallback.style.height = '100%';
  fallback.style.padding = '1rem';
  fallback.style.textAlign = 'center';
  fallback.style.fontSize = '0.875rem';
  fallback.style.color = '#cbd5f5';
  fallback.style.background = 'rgba(15, 23, 42, 0.85)';
  fallback.style.borderRadius = 'inherit';
  mapElement.appendChild(fallback);
}


function setDateInvalidState(picker, invalid) {
  if (!picker || !picker.altInput) return;
  picker.altInput.classList.toggle('is-invalid', invalid);
}

function placeMarker(lat, lon) {
  if (!leafletMap) return;
  const coords = [lat, lon];
  if (!activeMarker) {
    activeMarker = L.marker(coords, { draggable: true, autoPan: true }).addTo(leafletMap);
    activeMarker.on('moveend', (event) => {
      const pos = event.target.getLatLng();
      if (!isInGermany(pos.lat, pos.lng)) {
        leafletMap.removeLayer(activeMarker);
        activeMarker = null;
        showCoordinateError('Bitte bewege den Pin zurueck nach Deutschland.');
        resetCoordinateSummary();
        return;
      }
      showCoordinateError('');
      updateCoordinateSummary(pos.lat, pos.lng);
    });
  } else {
    activeMarker.setLatLng(coords);
  }
}

async function ensureLeafletReady() {
  if (typeof L !== 'undefined') return;
  injectLeafletFallback();
  for (let attempt = 0; attempt < MAX_LEAFLET_ATTEMPTS; attempt += 1) {
    if (typeof L !== 'undefined') return;
    await sleep(LEAFLET_RETRY_DELAY_MS);
  }
  throw new Error('Leaflet konnte nicht geladen werden.');
}

function initMap() {
  const mapElement = document.getElementById('map');
  if (!mapElement) return;

  leafletMap = L.map(mapElement, {
    zoomControl: false,
    minZoom: 5,
    maxZoom: 12,
    maxBounds: [[45.5, 4.5], [56.5, 16.5]],
    maxBoundsViscosity: 0.7,
  }).setView([51.163, 10.447], 6);

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: '&copy; OpenStreetMap-Mitwirkende',
  }).addTo(leafletMap);

  L.control.zoom({ position: 'bottomright' }).addTo(leafletMap);

  leafletMap.on('click', (event) => {
    const { lat, lng } = event.latlng;
    if (!isInGermany(lat, lng)) {
      showCoordinateError('Bitte waehle einen Punkt innerhalb Deutschlands.');
      resetCoordinateSummary();
      if (activeMarker) {
        leafletMap.removeLayer(activeMarker);
        activeMarker = null;
      }
      return;
    }
    showCoordinateError('');
    placeMarker(lat, lng);
    updateCoordinateSummary(lat, lng);
  });
}

function initDatePickers() {
  const startInput = document.getElementById('start_date');
  const endInput = document.getElementById('end_date');
  if (!startInput || !endInput) return;

  if (typeof flatpickr === 'undefined') {
    startInput.type = 'date';
    endInput.type = 'date';
    return;
  }

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const maxDate = new Date(today.getTime() - 24 * 60 * 60 * 1000);

  const sharedConfig = {
    altInput: true,
    altFormat: 'd.m.Y',
    dateFormat: 'Y-m-d',
    allowInput: true,
    disableMobile: true,
    locale: 'de',
    maxDate,
  };

  startPicker = flatpickr(startInput, {
    ...sharedConfig,
    onChange: (selectedDates, dateStr) => {
      if (endPicker) {
        endPicker.set('minDate', dateStr || null);
        if (selectedDates.length && endPicker.selectedDates.length && endPicker.selectedDates[0] < selectedDates[0]) {
          endPicker.clear();
        }
      }
      validateForm();
    },
    onClose: () => validateForm(),
    onValueUpdate: () => validateForm(),
  });

  endPicker = flatpickr(endInput, {
    ...sharedConfig,
    onChange: (selectedDates, dateStr) => {
      if (startPicker) {
        const newMax = dateStr || maxDate;
        startPicker.set('maxDate', newMax);
        if (selectedDates.length) {
          const selectedEnd = selectedDates[0];
          const currentStart = startPicker.selectedDates[0];
          if (!currentStart || currentStart > selectedEnd) {
            startPicker.setDate(selectedEnd, false);
          }
        }
      }
      validateForm();
    },
    onClose: () => validateForm(),
    onValueUpdate: () => validateForm(),
  });

  if (startPicker && startInput.value) {
    startPicker.setDate(startInput.value, false);
    endPicker.set('minDate', startInput.value);
  }

  if (endPicker && endInput.value) {
    endPicker.setDate(endInput.value, false);
    startPicker.set('maxDate', endInput.value);
  }

  const linkedInputs = [];
  if (startPicker && startPicker.altInput) linkedInputs.push(startPicker.altInput);
  if (endPicker && endPicker.altInput) linkedInputs.push(endPicker.altInput);
  linkedInputs.forEach((input) => {
    input.setAttribute('autocomplete', 'off');
    input.setAttribute('placeholder', 'TT.MM.JJJJ');
    input.classList.add('date-field');
  });

  validateForm();
}

function getStoredTheme() {
  try {
    const stored = window.localStorage?.getItem(THEME_STORAGE_KEY);
    if (stored === 'light' || stored === 'dark') return stored;
  } catch (err) {
    console.warn('Theme storage unavailable', err);
  }
  return null;
}

function setStoredTheme(theme) {
  try {
    window.localStorage?.setItem(THEME_STORAGE_KEY, theme);
  } catch (err) {
    console.warn('Theme storage unavailable', err);
  }
}

function updateThemeToggleUI(theme) {
  const toggle = document.getElementById('theme_toggle');
  if (!toggle) return;
  const isLight = theme === 'light';
  toggle.classList.toggle('is-light', isLight);
  toggle.setAttribute('aria-pressed', String(!isLight));
  toggle.setAttribute('aria-label', isLight
    ? 'Darstellung: Hellmodus aktiv (zum Umschalten klicken)'
    : 'Darstellung: Dunkelmodus aktiv (zum Umschalten klicken)');
  const textEl = toggle.querySelector('.theme-toggle__text');
  if (textEl) textEl.textContent = isLight ? 'Hell' : 'Dunkel';
}

function applyTheme(theme, { skipSave = false } = {}) {
  const body = document.body;
  if (!body) return;
  const normalized = theme === 'light' ? 'light' : 'dark';
  body.classList.remove('theme-dark', 'theme-light');
  body.classList.add(`theme-${normalized}`);
  updateThemeToggleUI(normalized);
  if (!skipSave) {
    setStoredTheme(normalized);
  }
}

function initThemeToggle() {
  const toggle = document.getElementById('theme_toggle');
  const storedTheme = getStoredTheme();
  const mediaQuery = typeof window.matchMedia === 'function'
    ? window.matchMedia('(prefers-color-scheme: dark)')
    : null;
  const initialTheme = storedTheme || (mediaQuery && mediaQuery.matches ? 'dark' : 'light');
  applyTheme(initialTheme, { skipSave: !storedTheme });

  if (toggle) {
    toggle.addEventListener('click', () => {
      const isDark = document.body.classList.contains('theme-dark');
      const nextTheme = isDark ? 'light' : 'dark';
      applyTheme(nextTheme);
    });
  }

  if (mediaQuery) {
    const handler = (event) => {
      if (getStoredTheme()) return;
      applyTheme(event.matches ? 'dark' : 'light', { skipSave: true });
    };
    if (typeof mediaQuery.addEventListener === 'function') {
      mediaQuery.addEventListener('change', handler);
    } else if (typeof mediaQuery.addListener === 'function') {
      mediaQuery.addListener(handler);
    }
  }
}

function validateForm() {
  let ok = true;

  const lat = parseFloat(latField.value);
  const lon = parseFloat(lonField.value);
  const coordsValid = Number.isFinite(lat) && Number.isFinite(lon) && isInGermany(lat, lon);
  if (!coordsValid) {
    showCoordinateError('Bitte setze den Pin innerhalb Deutschlands.');
    ok = false;
  } else {
    showCoordinateError('');
  }

  const start = document.getElementById('start_date');
  const end = document.getElementById('end_date');
  const startErr = document.getElementById('start_error');
  const endErr = document.getElementById('end_error');

  const startVal = start.value;
  const endVal = end.value;
  const startPast = isPastDateStr(startVal);
  const endPast = isPastDateStr(endVal);
  const orderOk = startVal && endVal && (new Date(startVal) <= new Date(endVal));

  const startValid = startVal && startPast;
  toggleClass(start, 'is-invalid', !startValid);
  toggleClass(startErr, 'hidden', startValid);
  setDateInvalidState(startPicker, !startValid);

  const endValid = endVal && endPast && orderOk;
  toggleClass(end, 'is-invalid', !endValid);
  toggleClass(endErr, 'hidden', endValid);
  setDateInvalidState(endPicker, !endValid);

  if (!startValid || !endValid) ok = false;
  return ok;
}

function showLoading(on) {
  const overlay = document.getElementById('loading_overlay');
  if (!overlay) return;
  overlay.classList.toggle('is-visible', on);
  overlay.setAttribute('aria-hidden', String(!on));
}

function initRadiusSlider() {
  const radiusEl = document.getElementById('radius');
  if (!radiusEl) return;
  radiusEl.addEventListener('input', (e) => {
    const lbl = document.getElementById('radius_value');
    if (lbl) lbl.textContent = e.target.value;
  });
}

function setupAnalyzeButton() {
  const button = document.getElementById('analyze_btn');
  if (!button) return;
  button.addEventListener('click', async () => {
    if (!resultBody) return;
    if (!validateForm()) {
      resultBody.textContent = 'Bitte Eingaben pruefen.';
      return;
    }

    const regionText = regionLabel?.textContent || 'Koordinaten';
    const addressLabel = regionText === REGION_LOADING_TEXT ? REGION_DEFAULT_TEXT : regionText;

    const payload = {
      address_1: addressLabel,
      lat: parseFloat(latField.value),
      lon: parseFloat(lonField.value),
      country_code: (countryField.value || '').toLowerCase(),
      radius: parseInt(document.getElementById('radius').value, 10),
      start_date: document.getElementById('start_date').value,
      end_date: document.getElementById('end_date').value,
    };

    showLoading(true);
    resultBody.textContent = '';
    try {
      const data = await postJSON('/api/analyze', payload);
      resultBody.textContent = data.summary || 'Keine Daten.';
    } catch (err) {
      resultBody.textContent = `Fehler: ${err.message}`;
    } finally {
      showLoading(false);
    }
  });
}

(async function boot() {
  initThemeToggle();
  initRadiusSlider();
  initDatePickers();
  setupAnalyzeButton();
  validateForm();
  try {
    await ensureLeafletReady();
    initMap();
  } catch (err) {
    console.error(err);
    markMapUnavailable('Kartendienst konnte nicht geladen werden. Bitte neu laden oder Netzwerkeinstellungen pruefen.');
  }
})();
