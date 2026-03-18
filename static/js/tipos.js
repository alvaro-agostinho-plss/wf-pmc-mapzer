const base = () => (window.BASE_PATH || "");
if (!getToken()) {
  window.location.href = base() + "/login";
  throw new Error("Redirecionando para login");
}

const tiposContainer = document.getElementById("tiposContainer");
const modalTipo = document.getElementById("modalTipo");
const modalTipoTitulo = document.getElementById("modalTipoTitulo");
const tipoId = document.getElementById("tipoId");
const tipoNome = document.getElementById("tipoNome");
const tipoStatus = document.getElementById("tipoStatus");
const btnNovoTipo = document.getElementById("btnNovoTipo");
const modalTipoSalvar = document.getElementById("modalTipoSalvar");
const modalTipoCancelar = document.getElementById("modalTipoCancelar");

function showLoad(msg = "Processando...") {
  const wrap = document.createElement("div");
  wrap.className = "load-overlay";
  wrap.id = "loadOverlay";
  wrap.innerHTML = `<div class="load-content"><div class="load-spinner"></div><span class="load-text">${escapeHtml(msg)}</span></div>`;
  document.body.appendChild(wrap);
}
function hideLoad() {
  document.getElementById("loadOverlay")?.remove();
}

function showToast(msg, type = "success") {
  if (type === "error") {
    showErrorModal(msg);
    return;
  }
  const t = document.createElement("div");
  t.className = `toast ${type}`;
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 4000);
}

function showErrorModal(msg) {
  const wrap = document.createElement("div");
  wrap.className = "error-modal-overlay";
  wrap.innerHTML = `
    <div class="error-modal">
      <div class="error-modal-text">${escapeHtml(msg)}</div>
      <button type="button" class="error-modal-btn">Fechar</button>
    </div>
  `;
  const close = () => wrap.remove();
  wrap.querySelector(".error-modal-btn").addEventListener("click", close);
  wrap.addEventListener("click", (e) => { if (e.target === wrap) close(); });
  document.body.appendChild(wrap);
}

function escapeHtml(s) {
  const d = document.createElement("div");
  d.textContent = s || "";
  return d.innerHTML;
}

async function loadTipos() {
  try {
    const r = await authFetch("/api/tipos");
    const items = await r.json();
    if (!items.length) {
      tiposContainer.innerHTML = '<p class="empty">Nenhum tipo cadastrado. Clique em "Novo tipo" para começar.</p>';
      tiposContainer.classList.remove("tipos-table");
    } else {
      tiposContainer.classList.add("tipos-table");
      tiposContainer.innerHTML = `
        <table class="lotes-grid tipos-grid">
          <colgroup>
            <col class="col-nome">
            <col class="col-status">
            <col class="col-acoes">
          </colgroup>
          <thead>
            <tr>
              <th>Nome</th>
              <th>Status</th>
              <th class="th-acoes">Ações</th>
            </tr>
          </thead>
          <tbody>
            ${items.map(t => renderTipoRow(t)).join("")}
          </tbody>
        </table>
      `;
      items.forEach(t => {
        document.querySelector(`[data-action="editar"][data-id="${t.id}"]`)?.addEventListener("click", () => abrirEditar(t.id));
        document.querySelector(`[data-action="excluir"][data-id="${t.id}"]`)?.addEventListener("click", () => excluirTipo(t.id, t.tip_nome));
      });
    }
  } catch (e) {
    tiposContainer.innerHTML = `<p class="empty">Erro: ${escapeHtml(e.message)}</p>`;
    tiposContainer.classList.remove("tipos-table");
  }
}

function renderTipoRow(t) {
  return `
    <tr class="lote-row" data-tipo-id="${t.id}">
      <td class="col-nome">${escapeHtml(t.tip_nome)}</td>
      <td class="col-status"><span class="status-badge status-${t.tip_status === "INATIVO" ? "inativo" : "ativo"}">${escapeHtml(t.tip_status === "INATIVO" ? "Inativo" : "Ativo")}</span></td>
      <td class="col-acoes">
        <button data-action="editar" data-id="${t.id}" class="btn-processar-lote btn-lote" title="Editar">Editar</button>
        <button data-action="excluir" data-id="${t.id}" class="btn-excluir-lote btn-lote" title="Excluir">Excluir</button>
      </td>
    </tr>
  `;
}

btnNovoTipo.addEventListener("click", () => {
  tipoId.value = "";
  tipoNome.value = "";
  tipoStatus.value = "ATIVO";
  modalTipoTitulo.textContent = "Novo tipo";
  modalTipo.style.display = "flex";
  tipoNome.focus();
});

modalTipoCancelar.addEventListener("click", () => {
  modalTipo.style.display = "none";
});

modalTipo.addEventListener("click", (e) => {
  if (e.target === modalTipo) modalTipoCancelar.click();
});

async function abrirEditar(id) {
  showLoad("Carregando...");
  try {
    const r = await authFetch(`/api/tipos/${id}`);
    const t = await r.json();
    tipoId.value = t.id;
    tipoNome.value = t.tip_nome || "";
    tipoStatus.value = t.tip_status === "INATIVO" ? "INATIVO" : "ATIVO";
    modalTipoTitulo.textContent = "Editar tipo";
    modalTipo.style.display = "flex";
    tipoNome.focus();
  } catch (e) {
    showToast(e.message || "Erro ao carregar tipo", "error");
  } finally {
    hideLoad();
  }
}

modalTipoSalvar.addEventListener("click", async () => {
  const nome = tipoNome.value.trim();
  if (!nome) {
    showToast("Nome é obrigatório", "error");
    return;
  }
  const id = tipoId.value;
  const isNovo = !id;
  modalTipoSalvar.disabled = true;
  showLoad(isNovo ? "Salvando..." : "Atualizando...");
  try {
    const body = { tip_nome: nome, tip_status: tipoStatus.value || "ATIVO" };
    const url = isNovo ? "/api/tipos" : `/api/tipos/${id}`;
    const method = isNovo ? "POST" : "PUT";
    const r = await authFetch(url, {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || data.msg || "Erro ao salvar");
    showToast(isNovo ? "Tipo criado" : "Tipo atualizado", "success");
    modalTipo.style.display = "none";
    loadTipos();
  } catch (e) {
    showToast(e.message || "Erro ao salvar", "error");
  } finally {
    hideLoad();
    modalTipoSalvar.disabled = false;
  }
});

async function excluirTipo(id, nome) {
  if (!confirm(`Excluir o tipo "${escapeHtml(nome)}"? Este tipo pode estar vinculado a ocorrências e setores.`)) return;
  showLoad("Excluindo...");
  try {
    const r = await authFetch(`/api/tipos/${id}`, { method: "DELETE" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || "Erro ao excluir");
    showToast("Tipo excluído", "success");
    loadTipos();
  } catch (e) {
    showToast(e.message || "Erro ao excluir", "error");
  } finally {
    hideLoad();
  }
}

document.getElementById("btnLogout")?.addEventListener("click", (e) => {
  e.preventDefault();
  removeToken();
  window.location.href = base() + "/login";
});

async function loadUserInfo() {
  const el = document.getElementById("userInfo");
  if (!el) return;
  try {
    const r = await authFetch("/api/auth/me");
    const u = await r.json();
    const nome = u.name || u.username || u.sub || "";
    el.textContent = nome ? `Bem-vindo, ${nome}` : "";
  } catch (_) {
    el.textContent = "";
  }
}

loadUserInfo();
loadTipos();
