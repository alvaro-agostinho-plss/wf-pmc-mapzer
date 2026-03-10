/**
 * Autenticação SSO - armazenamento e envio do token.
 */
const TOKEN_KEY = "mapzer_token";
const REFRESH_KEY = "mapzer_refresh";

function getToken() {
  return sessionStorage.getItem(TOKEN_KEY);
}

function getRefreshToken() {
  return sessionStorage.getItem(REFRESH_KEY);
}

function setToken(token) {
  sessionStorage.setItem(TOKEN_KEY, token);
}

function setRefreshToken(token) {
  if (token) sessionStorage.setItem(REFRESH_KEY, token);
}

function setTokens(accessToken, refreshToken) {
  setToken(accessToken);
  setRefreshToken(refreshToken || getRefreshToken());
}

function removeToken() {
  sessionStorage.removeItem(TOKEN_KEY);
  sessionStorage.removeItem(REFRESH_KEY);
}

function authHeaders() {
  const t = getToken();
  return t ? { Authorization: `Bearer ${t}` } : {};
}

function showSessaoExpirada() {
  const wrap = document.createElement("div");
  wrap.className = "error-modal-overlay sessao-expirada";
  wrap.innerHTML = `
    <div class="error-modal">
      <div class="error-modal-text">Sessão expirada. Redirecionando para o login...</div>
    </div>
  `;
  document.body.appendChild(wrap);
}

function baseUrl() {
  return (typeof window !== "undefined" && window.BASE_PATH) || "";
}

function redirectToLogin() {
  removeToken();
  showSessaoExpirada();
  setTimeout(() => {
    window.location.href = baseUrl() + "/login";
  }, 2500);
}

async function tryRefresh() {
  const refresh = getRefreshToken();
  if (!refresh) return false;
  try {
    const r = await fetch(baseUrl() + "/api/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: refresh }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data.access_token) return false;
    setTokens(data.access_token, data.refresh_token);
    return true;
  } catch (_) {
    return false;
  }
}

/**
 * fetch que inclui token. Em 401: tenta refresh; se falhar, mostra "Sessão expirada" e redireciona.
 */
async function authFetch(url, options = {}) {
  const fullUrl = url.startsWith("http") ? url : (baseUrl() + url);
  const headers = { ...(options.headers || {}), ...authHeaders() };
  let r = await fetch(fullUrl, { ...options, headers });
  if (r.status === 401) {
    const ok = await tryRefresh();
    if (ok) {
      const retryHeaders = { ...(options.headers || {}), ...authHeaders() };
      r = await fetch(fullUrl, { ...options, headers: retryHeaders });
      if (r.status === 401) redirectToLogin();
    } else {
      redirectToLogin();
    }
    throw new Error("Sessão expirada.");
  }
  return r;
}
