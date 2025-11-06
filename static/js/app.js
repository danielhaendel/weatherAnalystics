// Keep variable names and comments in English.

const DEFAULT_JS_TEXT = {
  regionLoading: 'Loading location ...',
  regionDefault: 'Germany',
  regionEmpty: 'No selection yet',
  markerReturnGermany: 'Please move the pin back within Germany.',
  placeWithinGermany: 'Please choose a point within Germany.',
  validateSelectGermany: 'Please place the pin within Germany.',
  analyzeCheckInputs: 'Please review your inputs.',
  analysisNoData: 'No data.',
  analysisErrorPrefix: 'Error',
  mapUnavailable: 'Map service could not be loaded. Please reload or check your network settings.',
  leafletLoadError: 'Leaflet could not be loaded.',
  tileAttribution: '&copy; OpenStreetMap contributors',
  regionFallbackLabel: 'Coordinates',
  themeToggleAriaDark: 'Display: dark mode active (click to switch)',
  themeToggleAriaLight: 'Display: light mode active (click to switch)',
  themeToggleDarkText: 'Dark',
  themeToggleLightText: 'Light',
  loadingOverlayText: 'Preparing weather data ...',
  datePlaceholder: 'MM/DD/YYYY',
  flatpickrAltFormat: 'm/d/Y',
  flatpickrDateFormat: 'Y-m-d',
  flatpickrLocale: 'en',
  syncStarted: 'Updating station data ...',
  syncSuccess: 'Station data updated.',
  syncUpToDate: 'Station data already up to date.',
  syncMissing: 'Station file missing on server.',
  syncListingEmpty: 'Data source unavailable.',
  syncError: 'Update failed',
  syncErrorDetails: 'Failed to refresh station data',
  toastCloseAria: 'Dismiss notification',
  regionStationUnknown: 'No station data available',
  regionStationDistance: 'Distance: {distance} km',
};

const APP_I18N = window.APP_I18N || {};
const JS_STRINGS = APP_I18N.js || {};
const CURRENT_LANG = APP_I18N.lang || JS_STRINGS.lang || 'en';
const TEXT = { ...DEFAULT_JS_TEXT, ...JS_STRINGS };

async function postJSON(url, payload) {
    const resp = await fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
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
const regionTitleEl = document.getElementById('coord_region_title');
const regionMetaEl = document.getElementById('coord_region_meta');
const regionStationNameEl = document.getElementById('coord_region_station');
const regionStationBlockEl = document.getElementById('coord_region_station_block');
const regionStationDistanceEl = document.getElementById('coord_region_distance');
const toastContainer = document.getElementById('toast_container');

let activeMarker = null;
let leafletMap = null;
let startPicker = null;
let endPicker = null;

const REGION_LOADING_TEXT = TEXT.regionLoading;
const REGION_DEFAULT_TEXT = TEXT.regionDefault;
const REGION_EMPTY_TEXT = TEXT.regionEmpty;
const THEME_STORAGE_KEY = "weather_theme_preference";
const REGION_STATION_UNKNOWN = TEXT.regionStationUnknown || '--';
const REGION_TITLE_DEFAULT = regionTitleEl?.dataset?.default || REGION_EMPTY_TEXT;
const REGION_META_DEFAULT = regionMetaEl?.dataset?.default || REGION_DEFAULT_TEXT;
const REGION_STATION_DEFAULT = regionStationNameEl?.dataset?.default || REGION_STATION_UNKNOWN;
const coverageInfo = window.APP_COVERAGE || null;

let regionLookupSeq = 0;
let coordErrorToastClose = null;

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


function setRegionLabel(title, { meta, stationName, stationDistance } = {}) {
  const resolvedTitle = title || REGION_TITLE_DEFAULT;
  const resolvedMeta = meta || REGION_META_DEFAULT;
  const resolvedStation = stationName || REGION_STATION_DEFAULT;

  if (regionTitleEl) regionTitleEl.textContent = resolvedTitle;
  if (regionMetaEl) regionMetaEl.textContent = resolvedMeta;
  if (regionStationNameEl) regionStationNameEl.textContent = resolvedStation;

  const hasStationDetails = Boolean(stationDistance) ||
    (resolvedStation && resolvedStation !== REGION_STATION_DEFAULT);

  if (regionStationBlockEl) {
    regionStationBlockEl.classList.toggle('hidden', !hasStationDetails);
  }

  if (regionStationDistanceEl) {
    if (stationDistance) {
      regionStationDistanceEl.textContent = stationDistance;
      regionStationDistanceEl.classList.remove('hidden');
    } else {
      regionStationDistanceEl.textContent = '';
      regionStationDistanceEl.classList.add('hidden');
    }
  }
}

async function resolveRegionName(lat, lon) {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

  regionLookupSeq += 1;
  const lookupId = regionLookupSeq;
  setRegionLabel(REGION_LOADING_TEXT, {
    meta: REGION_META_DEFAULT,
    stationName: REGION_STATION_DEFAULT,
  });

  try {
    const resp = await fetch(`/api/reverse_geocode?lat=${lat}&lon=${lon}&lang=${encodeURIComponent(CURRENT_LANG)}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    if (lookupId !== regionLookupSeq) return;

    const city = data.city || data.town || data.village || data.municipality
      || data.locality || data.city_district || data.place || data.name;
    const stateName = data.state || data.region || data.county;
    const stationInfo = await fetchNearestStationInfo(lat, lon, lookupId);

    const displayTitle = city || stationInfo?.title || REGION_DEFAULT_TEXT;
    const metaParts = new Set();
    if (stateName) metaParts.add(stateName);
    if (!stateName && stationInfo?.state) metaParts.add(stationInfo.state);
    metaParts.add(REGION_DEFAULT_TEXT);
    const meta = Array.from(metaParts).filter(Boolean).join(', ');

    setRegionLabel(displayTitle, {
      meta,
      stationName: stationInfo?.stationDisplay || REGION_STATION_DEFAULT,
      stationDistance: stationInfo?.distanceText || '',
    });
  } catch (err) {
    if (lookupId === regionLookupSeq) {
      fetchNearestStationInfo(lat, lon, lookupId)
        .then((info) => {
          if (lookupId !== regionLookupSeq) return;
          if (info) {
            const metaParts = new Set();
            if (info.state) metaParts.add(info.state);
            metaParts.add(REGION_DEFAULT_TEXT);
            setRegionLabel(info.title || REGION_DEFAULT_TEXT, {
              meta: Array.from(metaParts).filter(Boolean).join(', '),
              stationName: info.stationDisplay || REGION_STATION_DEFAULT,
              stationDistance: info.distanceText || '',
            });
          } else {
            setRegionLabel(REGION_DEFAULT_TEXT, {
              meta: REGION_META_DEFAULT,
              stationName: REGION_STATION_DEFAULT,
              stationDistance: '',
            });
          }
        })
        .catch(() => {
          if (lookupId !== regionLookupSeq) return;
          setRegionLabel(REGION_DEFAULT_TEXT, {
            meta: REGION_META_DEFAULT,
            stationName: REGION_STATION_DEFAULT,
            stationDistance: '',
          });
        });
    }
    console.error('Failed to resolve region name', err);
  }
}

async function fetchNearestStationInfo(lat, lon, lookupId) {
  try {
    const resp = await fetch(`/api/stations/nearest?lat=${lat}&lon=${lon}`);
    if (!resp.ok) return null;
    const data = await resp.json();
    if (lookupId && lookupId !== regionLookupSeq) return null;
    if (data?.station) {
      const station = data.station;
      const distanceValue = station.distance_km != null ? parseFloat(station.distance_km) : null;
      return {
        title: [station.name, station.state].filter(Boolean).join(', ') || station.name || station.station_id,
        state: station.state || null,
        stationDisplay: station.name
          ? (station.state ? `${station.name} · ${station.state}` : station.name)
          : REGION_STATION_DEFAULT,
        distance: Number.isFinite(distanceValue) ? distanceValue : null,
        distanceText: Number.isFinite(distanceValue) ? formatStationDistance(distanceValue) : '',
      };
    }
  } catch (err) {
    console.warn('Fallback station lookup failed', err);
  }
  return null;
}

function formatStationDistance(distance) {
  if (!Number.isFinite(distance)) return '';
  const template = TEXT.regionStationDistance || '{distance} km';
  const formatted = distance.toFixed(2);
  return template.replace('{distance}', formatted);
}

function getRegionDisplayText() {
  const titleRaw = regionTitleEl?.textContent?.trim();
  const metaRaw = regionMetaEl?.textContent?.trim();
  const title = titleRaw && titleRaw !== REGION_TITLE_DEFAULT ? titleRaw : '';
  const meta = metaRaw && metaRaw !== REGION_META_DEFAULT ? metaRaw : REGION_META_DEFAULT;
  if (title) {
    return meta ? `${title}, ${meta}` : title;
  }
  return meta || REGION_DEFAULT_TEXT;
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
    setRegionLabel(REGION_EMPTY_TEXT, {
      meta: REGION_META_DEFAULT,
      stationName: REGION_STATION_DEFAULT,
      stationDistance: '',
    });
}

function showCoordinateError(message) {
    if (coordErrorToastClose) {
        coordErrorToastClose();
        coordErrorToastClose = null;
    }
    if (!message) {
        return;
    }
    coordErrorToastClose = showToast({
        kind: 'warning',
        message,
        autoDismiss: true,
        duration: 5000,
    });
}

function markMapUnavailable(message = TEXT.mapUnavailable) {
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
        activeMarker = L.marker(coords, {draggable: true, autoPan: true}).addTo(leafletMap);
        activeMarker.on('moveend', (event) => {
            const pos = event.target.getLatLng();
            if (!isInGermany(pos.lat, pos.lng)) {
                leafletMap.removeLayer(activeMarker);
                activeMarker = null;
                showCoordinateError(TEXT.markerReturnGermany);
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
    throw new Error(TEXT.leafletLoadError);
}

function initMap() {
    const mapElement = document.getElementById('map');
    if (!mapElement) return;

    leafletMap = L.map(mapElement, {
        zoomControl: false,
        minZoom: 5.3,
        maxZoom: 16,
        maxBounds: [[45.5, 4.5], [56.5, 16.5]],
        maxBoundsViscosity: 0.7,
    }).setView([51.163, 10.447], 6);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 18,
        attribution: TEXT.tileAttribution,
    }).addTo(leafletMap);

    L.control.zoom({position: 'bottomright'}).addTo(leafletMap);

    leafletMap.on('click', (event) => {
        const {lat, lng} = event.latlng;
        if (!isInGermany(lat, lng)) {
            showCoordinateError(TEXT.placeWithinGermany);
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

    const minDateStr = coverageInfo?.min_date || null;
    const maxDateStr = coverageInfo?.max_date || null;

    if (typeof flatpickr === 'undefined') {
        startInput.type = 'date';
        if (minDateStr) startInput.min = minDateStr;
        if (maxDateStr) startInput.max = maxDateStr;
        endInput.type = 'date';
        if (minDateStr) endInput.min = minDateStr;
        if (maxDateStr) endInput.max = maxDateStr;
        return;
    }

    const resolvedLocale = (!TEXT.flatpickrLocale || TEXT.flatpickrLocale === 'default')
        ? 'en'
        : TEXT.flatpickrLocale;
    const sharedConfig = {
        altInput: true,
        altFormat: TEXT.flatpickrAltFormat,
        dateFormat: TEXT.flatpickrDateFormat,
        allowInput: true,
        disableMobile: true,
        locale: resolvedLocale,
        minDate: minDateStr,
        maxDate: maxDateStr,
    };

    startPicker = flatpickr(startInput, {
        ...sharedConfig,
        onChange: (selectedDates, dateStr) => {
            if (endPicker) {
                endPicker.set('minDate', dateStr || minDateStr || null);
                if (selectedDates.length && endPicker.selectedDates.length && endPicker.selectedDates[0] < selectedDates[0]) {
                    endPicker.clear();
                }
            }
            validateForm({ suppressCoordinateToast: true });
        },
        onClose: () => validateForm({ suppressCoordinateToast: true }),
        onValueUpdate: () => validateForm({ suppressCoordinateToast: true }),
    });

    endPicker = flatpickr(endInput, {
        ...sharedConfig,
        onChange: (selectedDates, dateStr) => {
            if (startPicker) {
                const newMax = dateStr || maxDateStr || null;
                startPicker.set('maxDate', newMax);
                if (selectedDates.length) {
                    const selectedEnd = selectedDates[0];
                    const currentStart = startPicker.selectedDates[0];
                    if (!currentStart || currentStart > selectedEnd) {
                        startPicker.setDate(selectedEnd, false);
                    }
                }
            }
            validateForm({ suppressCoordinateToast: true });
        },
        onClose: () => validateForm({ suppressCoordinateToast: true }),
        onValueUpdate: () => validateForm({ suppressCoordinateToast: true }),
    });

    if (startPicker && startInput.value) {
        startPicker.setDate(startInput.value, false);
        endPicker.set('minDate', startInput.value);
    }

    if (endPicker && endInput.value) {
        endPicker.setDate(endInput.value, false);
        startPicker.set('maxDate', endInput.value);
    }

    if (coverageInfo && maxDateStr) {
        const endDateObj = new Date(`${maxDateStr}T00:00:00`);
        const startDateObj = new Date(endDateObj.getTime());
        startDateObj.setDate(startDateObj.getDate() - 30);
        const isoStart = startDateObj.toISOString().slice(0, 10);
        const clampedStart = minDateStr && isoStart < minDateStr ? minDateStr : isoStart;
        if (startPicker) {
            startPicker.setDate(clampedStart, false);
            startPicker.set('maxDate', maxDateStr);
        }
        if (endPicker) {
            endPicker.setDate(maxDateStr, false);
            endPicker.set('minDate', clampedStart);
        }
    }

    const linkedInputs = [];
    if (startPicker && startPicker.altInput) linkedInputs.push(startPicker.altInput);
    if (endPicker && endPicker.altInput) linkedInputs.push(endPicker.altInput);
    linkedInputs.forEach((input) => {
        input.setAttribute('autocomplete', 'off');
        input.setAttribute('placeholder', TEXT.datePlaceholder);
        input.classList.add('date-field');
    });

    validateForm({ suppressCoordinateToast: true });
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
    toggle.setAttribute('aria-label', isLight ? TEXT.themeToggleAriaLight : TEXT.themeToggleAriaDark);
    const sunIcon = toggle.querySelector('[data-icon="sun"]');
    const moonIcon = toggle.querySelector('[data-icon="moon"]');
    if (sunIcon) sunIcon.classList.toggle('hidden', !isLight);
    if (moonIcon) moonIcon.classList.toggle('hidden', isLight);
}

function applyTheme(theme, {skipSave = false} = {}) {
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
    applyTheme(initialTheme, {skipSave: !storedTheme});

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
            applyTheme(event.matches ? 'dark' : 'light', {skipSave: true});
        };
        if (typeof mediaQuery.addEventListener === 'function') {
            mediaQuery.addEventListener('change', handler);
        } else if (typeof mediaQuery.addListener === 'function') {
            mediaQuery.addListener(handler);
        }
    }
}

function validateForm(options = {}) {
    const { suppressCoordinateToast = false } = options;
    let ok = true;

    const lat = parseFloat(latField.value);
    const lon = parseFloat(lonField.value);
    const coordsValid = Number.isFinite(lat) && Number.isFinite(lon) && isInGermany(lat, lon);
    if (!coordsValid) {
        if (!suppressCoordinateToast) {
            showCoordinateError(TEXT.validateSelectGermany);
        }
        ok = false;
    } else if (!suppressCoordinateToast) {
        showCoordinateError('');
    }

    const start = document.getElementById('start_date');
    const end = document.getElementById('end_date');
    const startErr = document.getElementById('start_error');
    const endErr = document.getElementById('end_error');

    const startVal = start.value || '';
    const endVal = end.value || '';

    let datesValid = Boolean(startVal && endVal);
    if (datesValid) {
        if (startVal > endVal) {
            datesValid = false;
        }
        if (coverageInfo) {
            if (startVal < coverageInfo.min_date || endVal > coverageInfo.max_date) {
                datesValid = false;
            }
        }
    }

    toggleClass(start, 'is-invalid', !datesValid || !startVal);
    toggleClass(startErr, 'hidden', datesValid && Boolean(startVal));
    setDateInvalidState(startPicker, !(datesValid && Boolean(startVal)));

    toggleClass(end, 'is-invalid', !datesValid || !endVal);
    toggleClass(endErr, 'hidden', datesValid && Boolean(endVal));
    setDateInvalidState(endPicker, !(datesValid && Boolean(endVal)));

    if (!datesValid) {
        ok = false;
    }

    if (coordsValid && suppressCoordinateToast) {
        showCoordinateError('');
    }
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

function initSyncButton() {
  const button = document.getElementById('sync_button');
  if (!button) return;
  const defaultLabel = button.textContent.trim();

  const statusMessages = {
    downloaded: TEXT.syncSuccess,
    up_to_date: TEXT.syncUpToDate || TEXT.syncSuccess,
    missing: TEXT.syncMissing || TEXT.syncErrorDetails,
    listing_empty: TEXT.syncListingEmpty || TEXT.syncErrorDetails,
    unknown: TEXT.syncErrorDetails,
  };

  const restoreDefault = () => {
    button.disabled = false;
    button.textContent = defaultLabel;
  };

  button.addEventListener('click', async () => {
    if (button.disabled) return;
    button.disabled = true;
    button.textContent = TEXT.syncStarted;
    let dismissToast = showToast({
      kind: 'info',
      title: TEXT.syncStarted,
      message: TEXT.syncStarted,
      autoDismiss: false,
    });
    try {
      const data = await postJSON('/api/sync_stations', {});
      const rows = data?.stations?.rows_processed || 0;
      const downloaded = Boolean(data?.stations?.downloaded);
      const messageKey = data?.stations?.message || (downloaded ? 'downloaded' : 'unknown');
      const messageText = statusMessages[messageKey] || statusMessages.unknown;
      const formatted = downloaded && rows
        ? `${messageText} (${rows})`
        : messageText;
      button.textContent = formatted;
      const variant = downloaded
        ? 'success'
        : (messageKey === 'up_to_date' ? 'info' : (messageKey === 'missing' || messageKey === 'listing_empty' ? 'warning' : 'error'));
      if (typeof dismissToast === 'function') dismissToast();
      dismissToast = showToast({
        kind: variant,
        title: formatted,
        message: rows ? `${rows} ${TEXT.syncSuccess}` : formatted,
        autoDismiss: true,
      });
    } catch (err) {
      console.error('Station sync failed', err);
      const errorMessage = `${TEXT.syncError}: ${err.message}`;
      button.textContent = TEXT.syncError;
      if (typeof dismissToast === 'function') dismissToast();
      dismissToast = showToast({
        kind: 'error',
        title: TEXT.syncError,
        message: err?.message || TEXT.syncErrorDetails,
        autoDismiss: false,
      });
    } finally {
      setTimeout(restoreDefault, 2500);
    }
  });
}

function initLanguageSelector() {
    const trigger = document.getElementById('language_menu_button');
    const menu = document.getElementById('language_menu');
    if (!trigger || !menu) return;

    const items = Array.from(menu.querySelectorAll('[data-lang]'));
    if (!items.length) return;

    let isOpen = false;

    const setOpen = (state) => {
        isOpen = state;
        trigger.setAttribute('aria-expanded', String(state));
        menu.classList.toggle('hidden', !state);
        if (state) {
            const active = menu.querySelector('.language-menu__item.is-active');
            (active || items[0]).focus();
        }
    };

    const closeMenu = () => setOpen(false);
    const openMenu = () => setOpen(true);

    trigger.addEventListener('click', (event) => {
        event.preventDefault();
        if (isOpen) {
            closeMenu();
        } else {
            openMenu();
        }
    });

    trigger.addEventListener('keydown', (event) => {
        if (['ArrowDown', 'Enter', ' '].includes(event.key)) {
            event.preventDefault();
            if (!isOpen) openMenu();
        }
    });

    const handleDocumentClick = (event) => {
        if (!isOpen) return;
        if (trigger.contains(event.target) || menu.contains(event.target)) return;
        closeMenu();
    };

    const handleDocumentKeydown = (event) => {
        if (event.key === 'Escape' && isOpen) {
            closeMenu();
            trigger.focus();
        }
    };

    document.addEventListener('click', handleDocumentClick);
    document.addEventListener('keydown', handleDocumentKeydown);

    const focusItem = (index) => {
        const clamped = ((index % items.length) + items.length) % items.length;
        items[clamped].focus();
    };

    items.forEach((item, index) => {
        item.addEventListener('click', () => {
            const nextLang = item.dataset.lang;
            closeMenu();
            if (!nextLang || nextLang === CURRENT_LANG) return;
            const url = new URL(window.location.href);
            url.searchParams.set('lang', nextLang);
            window.location.href = url.toString();
        });

        item.addEventListener('keydown', (event) => {
            if (event.key === 'ArrowDown') {
                event.preventDefault();
                focusItem(index + 1);
            } else if (event.key === 'ArrowUp') {
                event.preventDefault();
                focusItem(index - 1);
            } else if (event.key === 'Home') {
                event.preventDefault();
                focusItem(0);
            } else if (event.key === 'End') {
                event.preventDefault();
                focusItem(items.length - 1);
            }
        });
    });
}

function setupAnalyzeButton() {
  const button = document.getElementById('analyze_btn');
  if (!button) return;
  button.addEventListener('click', () => {
    if (!validateForm()) {
      showToast({ kind: 'warning', message: TEXT.analyzeCheckInputs, duration: 4000 });
      return;
    }

    const form = document.getElementById('report_form');
    if (!form) return;

    form.elements.namedItem('lat').value = latField.value;
    form.elements.namedItem('lon').value = lonField.value;
    form.elements.namedItem('radius').value = document.getElementById('radius').value;
    form.elements.namedItem('start_date').value = document.getElementById('start_date').value;
    form.elements.namedItem('end_date').value = document.getElementById('end_date').value;
    form.elements.namedItem('granularity').value = document.getElementById('granularity').value;

    showLoading(true);
    form.submit();
  });
}

(async function boot() {
  initSyncButton();
  initLanguageSelector();
  initThemeToggle();
    initRadiusSlider();
    initDatePickers();
    setupAnalyzeButton();
    validateForm({ suppressCoordinateToast: true });
    try {
        await ensureLeafletReady();
        initMap();
    } catch (err) {
        console.error(err);
        markMapUnavailable();
    }
})();

function createToastNode({ kind = 'info', title = '', message = '', autoDismiss = false, duration = 6000 }) {
  const card = document.createElement('div');
  card.className = `toast-card toast-card--${kind}`;
  card.setAttribute('role', 'status');

  const icon = document.createElement('div');
  icon.className = 'toast-card__icon';
  icon.textContent = {
    success: '✓',
    info: 'ℹ',
    warning: '⚠',
    error: '⛔',
  }[kind] || 'ℹ';

  const content = document.createElement('div');
  content.className = 'toast-card__content';

  if (title) {
    const titleEl = document.createElement('div');
    titleEl.className = 'toast-card__title';
    titleEl.textContent = title;
    content.appendChild(titleEl);
  }

  if (message) {
    const messageEl = document.createElement('div');
    messageEl.className = 'toast-card__message';
    messageEl.textContent = message;
    content.appendChild(messageEl);
  }

  const closeButton = document.createElement('button');
  closeButton.className = 'toast-card__close';
  closeButton.setAttribute('type', 'button');
  closeButton.setAttribute('aria-label', TEXT.toastCloseAria || 'Dismiss');
  closeButton.innerHTML = '&times;';

  const removeToast = () => {
    card.style.animation = 'toast-exit 0.28s ease forwards';
    setTimeout(() => card.remove(), 260);
  };

  closeButton.addEventListener('click', removeToast);

  card.appendChild(icon);
  card.appendChild(content);
  card.appendChild(closeButton);

  if (autoDismiss) {
    setTimeout(removeToast, duration);
  }

  return { card, removeToast };
}

function showToast(options) {
  if (!toastContainer) return () => {};
  const { card, removeToast } = createToastNode(options);
  toastContainer.appendChild(card);
  return removeToast;
}
