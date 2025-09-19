// Keep all variable names and comments in English.

function debounce(fn, ms) {
  let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

async function getJSON(url) {
  const r = await fetch(url, { headers: { 'Accept': 'application/json' } });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

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
  if (!el) return; // guard
  el.classList.toggle(className, on);
}

function setHiddenCoords(lat, lon, countryCode) {
  document.getElementById('address_lat').value = lat ?? '';
  document.getElementById('address_lon').value = lon ?? '';
  const ccEl = document.getElementById('address_country');
  if (ccEl) ccEl.value = (countryCode || '').toLowerCase();
}

function showSuggestions(items, errorMsg) {
  const box = document.getElementById('address_suggestions');
  box.innerHTML = '';
  box.dataset.items = JSON.stringify(items || []);

  if (errorMsg) {
    const warn = document.createElement('div');
    warn.className = 'list-group-item text-danger small';
    warn.textContent = `Hinweis: ${errorMsg}`;
    box.appendChild(warn);
  }

  (items || []).forEach((it) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'list-group-item list-group-item-action';
    btn.textContent = it.label;
    btn.addEventListener('mousedown', (e) => { applySelection(it); e.preventDefault(); });
    box.appendChild(btn);
  });

  toggleClass(box, 'd-none', !(items && items.length) && !errorMsg);
}

function applySelection(item) {
  const input = document.getElementById('address_1');
  input.value = item.label;
  input.classList.remove('is-invalid');
  setHiddenCoords(item.lat, item.lon, item.country_code);
  document.getElementById('address_suggestions').classList.add('d-none');
  const err = document.getElementById('address_error');
  if (err) err.classList.add('d-none');
}

const onAddressInput = debounce(async () => {
  const q = document.getElementById('address_1').value.trim();
  setHiddenCoords('', '', '');
  if (!q) { showSuggestions([]); return; }
  try {
    const data = await getJSON(`/api/places?q=${encodeURIComponent(q)}&country=de`);
    showSuggestions(data.items || [], data.error);
  } catch {
    showSuggestions([], 'Serverfehler');
  }
}, 250);

document.getElementById('address_1').addEventListener('input', onAddressInput);
document.getElementById('address_1').addEventListener('focus', onAddressInput);
document.addEventListener('click', (e) => {
  const box = document.getElementById('address_suggestions');
  if (!box.contains(e.target) && e.target.id !== 'address_1') box.classList.add('d-none');
});

// Keyboard navigation (optional)
document.getElementById('address_1').addEventListener('keydown', (e) => {
  const box = document.getElementById('address_suggestions');
  if (box.classList.contains('d-none')) return;
  const items = Array.from(box.querySelectorAll('.list-group-item'));
  let idx = items.findIndex(el => el.classList.contains('active'));
  if (e.key === 'ArrowDown') {
    idx = (idx + 1) % items.length;
    items.forEach(el => el.classList.remove('active'));
    items[idx].classList.add('active');
    e.preventDefault();
  } else if (e.key === 'ArrowUp') {
    idx = (idx - 1 + items.length) % items.length;
    items.forEach(el => el.classList.remove('active'));
    items[idx].classList.add('active');
    e.preventDefault();
  } else if (e.key === 'Enter') {
    if (idx >= 0) {
      const list = JSON.parse(box.dataset.items || '[]');
      if (list[idx]) applySelection(list[idx]);
      e.preventDefault();
    }
  } else if (e.key === 'Escape') {
    box.classList.add('d-none');
  }
});

// Radius label
const radiusEl = document.getElementById('radius');
if (radiusEl) radiusEl.addEventListener('input', e => {
  const lbl = document.getElementById('radius_value');
  if (lbl) lbl.textContent = e.target.value;
});

// Validation helpers
function isPastDateStr(iso) {
  if (!iso) return false;
  const today = new Date(); today.setHours(0,0,0,0);
  const d = new Date(iso + 'T00:00:00');
  return d < today;
}

function validateForm() {
  const input = document.getElementById('address_1');
  const addrErr = document.getElementById('address_error');
  const lat = document.getElementById('address_lat').value;
  const lon = document.getElementById('address_lon').value;
  const countryEl = document.getElementById('address_country');
  const country = countryEl ? countryEl.value : 'de'; // default to de if not present

  const start = document.getElementById('start_date');
  const end = document.getElementById('end_date');
  const startErr = document.getElementById('start_error');
  const endErr = document.getElementById('end_error');

  let ok = true;

  // Address: must have coords and be Germany
  const addrInvalid = !lat || !lon || country !== 'de';
  toggleClass(input, 'is-invalid', addrInvalid);
  toggleClass(addrErr, 'd-none', !addrInvalid);
  if (addrInvalid) ok = false;

  // Dates: both in past and start <= end
  const startVal = start.value;
  const endVal = end.value;
  const startPast = isPastDateStr(startVal);
  const endPast = isPastDateStr(endVal);
  const orderOk = startVal && endVal && (new Date(startVal) <= new Date(endVal));

  toggleClass(start, 'is-invalid', !(startVal && startPast));
  toggleClass(startErr, 'd-none', (startVal && startPast));

  toggleClass(end, 'is-invalid', !(endVal && endPast && orderOk));
  toggleClass(endErr, 'd-none', (endVal && endPast && orderOk));

  if (!(startVal && startPast && endVal && endPast && orderOk)) ok = false;

  return ok;
}

function showLoading(on) {
  const ov = document.getElementById('loading_overlay');
  toggleClass(ov, 'd-none', !on);
}

document.getElementById('analyze_btn').addEventListener('click', async () => {
  const resultBody = document.querySelector('#result .card-body');
  if (!validateForm()) {
    resultBody.textContent = 'Bitte Eingaben prüfen.';
    return;
  }

  const payload = {
    address_1: document.getElementById('address_1').value,
    lat: parseFloat(document.getElementById('address_lat').value),
    lon: parseFloat(document.getElementById('address_lon').value),
    country_code: (document.getElementById('address_country')?.value || '').toLowerCase(),
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
