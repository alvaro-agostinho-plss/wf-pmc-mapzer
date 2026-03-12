const base = () => (window.BASE_PATH || "");
if (!getToken()) {
  window.location.href = base() + "/login";
  throw new Error("Redirecionando para login");
}

const setoresContainer = document.getElementById("setoresContainer");
const modalSetor = document.getElementById("modalSetor");
const modalSetorTitulo = document.getElementById("modalSetorTitulo");
const formSetor = document.getElementById("formSetor");
const setorId = document.getElementById("setorId");
const setorNome = document.getElementById("setorNome");
const setorEmail = document.getElementById("setorEmail");
const setorWhatsapp = document.getElementById("setorWhatsapp");
const setorStatus = document.getElementById("setorStatus");
const tiposCheckboxes = document.getElementById("tiposCheckboxes");
const btnNovoSetor = document.getElementById("btnNovoSetor");
const modalSetorSalvar = document.getElementById("modalSetorSalvar");
const modalSetorCancelar = document.getElementById("modalSetorCancelar");

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

function renderTiposSemSetor(el, tipos, tiposMultiplosSetores = []) {
  if (!el) return;
  el.classList.remove("tipos-sem-setor-ok-box");
  if (!Array.isArray(tipos) || !tipos.length) {
    let html = '<span class="tipos-sem-setor-ok">Todos os tipos estão vinculados a setores.</span>';
    const mult = Array.isArray(tiposMultiplosSetores) ? tiposMultiplosSetores : [];
    const nomesMult = mult.map(t => String(t.tip_nome || t.nome || "").trim()).filter(Boolean);
    if (nomesMult.length) {
      const textoMult = nomesMult.map(escapeHtml).join(", ");
      html += `<br><span class="tipos-multiplos-setores">Tipos vinculados a mais de um setor: ${textoMult}</span>`;
    }
    el.innerHTML = html;
    el.style.display = "block";
    el.classList.add("tipos-sem-setor-ok-box");
    return;
  }
  const nomes = tipos.map(t => String(t.tip_nome || t.nome || "").trim()).filter(Boolean);
  if (!nomes.length) {
    let html = '<span class="tipos-sem-setor-ok">Todos os tipos estão vinculados a setores.</span>';
    const mult = Array.isArray(tiposMultiplosSetores) ? tiposMultiplosSetores : [];
    const nomesMult = mult.map(t => String(t.tip_nome || t.nome || "").trim()).filter(Boolean);
    if (nomesMult.length) {
      const textoMult = nomesMult.map(escapeHtml).join(", ");
      html += `<br><span class="tipos-multiplos-setores">Tipos vinculados a mais de um setor: ${textoMult}</span>`;
    }
    el.innerHTML = html;
    el.style.display = "block";
    el.classList.add("tipos-sem-setor-ok-box");
    return;
  }
  const texto = nomes.map(escapeHtml).join(", ");
  el.innerHTML = `<span class="tipos-sem-setor-label">Tipos sem associação com setor: </span><span class="tipos-sem-setor-lista">${texto}</span>`;
  el.style.display = "block";
}

async function loadSetores() {
  const elTiposSemSetor = document.getElementById("tiposSemSetor");
  try {
    const r = await authFetch("/api/setores");
    const data = await r.json();
    if (!r.ok) throw new Error(data.detail || "Erro ao carregar setores");
    const items = Array.isArray(data) ? data : [];
    let tiposSemVinculo = [];
    let tiposMultiplosSetores = [];
    try {
      const r2 = await authFetch("/api/setores/tipos-sem-vinculo");
      if (r2.ok) {
        const t = await r2.json();
        tiposSemVinculo = Array.isArray(t) ? t : [];
      }
    } catch (_) { /* tipos sem vínculo: manter vazio se falhar */ }
    try {
      const r3 = await authFetch("/api/setores/tipos-multiplos-setores");
      if (r3.ok) {
        const m = await r3.json();
        tiposMultiplosSetores = Array.isArray(m) ? m : [];
      }
    } catch (_) { /* tipos múltiplos setores: manter vazio se falhar */ }
    if (elTiposSemSetor) {
      renderTiposSemSetor(elTiposSemSetor, tiposSemVinculo, tiposMultiplosSetores);
    } else {
      const parent = document.querySelector(".acoes-lote")?.nextElementSibling?.parentElement || document.querySelector("main");
      if (parent) {
        const div = document.createElement("div");
        div.id = "tiposSemSetor";
        div.className = "tipos-sem-setor";
        parent.insertBefore(div, document.getElementById("setoresContainer")?.parentElement || parent.firstChild);
        renderTiposSemSetor(div, tiposSemVinculo, tiposMultiplosSetores);
      }
    }
    if (!items.length) {
      setoresContainer.innerHTML = '<p class="empty">Nenhum setor cadastrado. Clique em "Novo setor" para começar.</p>';
      setoresContainer.classList.remove("setores-table");
    } else {
      setoresContainer.classList.add("setores-table");
      setoresContainer.innerHTML = `
        <table class="lotes-grid setores-grid">
          <colgroup>
            <col class="col-nome">
            <col class="col-email">
            <col class="col-whatsapp">
            <col class="col-status">
            <col class="col-acoes">
          </colgroup>
          <thead>
            <tr>
              <th>Nome</th>
              <th>E-mail</th>
              <th>WhatsApp</th>
              <th>Status</th>
              <th class="th-acoes">Ações</th>
            </tr>
          </thead>
          <tbody>
            ${items.map(s => renderSetorRow(s)).join("")}
          </tbody>
        </table>
      `;
      items.forEach(s => {
        document.querySelector(`[data-action="editar"][data-id="${s.id}"]`)?.addEventListener("click", () => abrirEditar(s.id));
        document.querySelector(`[data-action="excluir"][data-id="${s.id}"]`)?.addEventListener("click", () => excluirSetor(s.id, s.set_nome));
        document.querySelector(`.btn-expand-setor[data-setor-id="${s.id}"]`)?.addEventListener("click", (e) => {
          e.stopPropagation();
          const detailRow = document.querySelector(`.setor-detail-row[data-setor-id="${s.id}"]`);
          const icon = e.currentTarget.querySelector("i");
          if (detailRow && icon) {
            const isHidden = detailRow.style.display === "none" || !detailRow.style.display;
            detailRow.style.display = isHidden ? "table-row" : "none";
            const temTipos = (s.tip_nomes || []).length > 0;
            icon.className = `fa-solid fa-chevron-${isHidden ? "up" : (temTipos ? "down" : "right")}`;
          }
        });
      });
    }
  } catch (e) {
    setoresContainer.innerHTML = `<p class="empty">Erro: ${escapeHtml(e.message)}</p>`;
    setoresContainer.classList.remove("setores-table");
    if (elTiposSemSetor) elTiposSemSetor.innerHTML = "";
  }
}

function renderSetorRow(s) {
  const tipNomes = Array.isArray(s.tip_nomes) ? s.tip_nomes : [];
  const temTipos = tipNomes.length > 0;
  const tiposTexto = tipNomes.map(escapeHtml).join(", ");
  return `
    <tr class="lote-row setor-row" data-setor-id="${s.id}">
      <td class="col-nome">
        <button type="button" class="btn-expand-setor" data-setor-id="${s.id}" data-tem-tipos="${temTipos}" title="${temTipos ? "Expandir tipos" : "Sem tipos vinculados"}">
          <i class="fa-solid fa-chevron-${temTipos ? "down" : "right"}"></i>
        </button>
        ${escapeHtml(s.set_nome)}
      </td>
      <td class="col-email">${escapeHtml(s.set_email || "—")}</td>
      <td class="col-whatsapp">${escapeHtml(s.set_whatsapp || "—")}</td>
      <td class="col-status">${escapeHtml(s.set_status === "INATIVO" ? "Inativo" : "Ativo")}</td>
      <td class="col-acoes">
        <button data-action="editar" data-id="${s.id}" class="btn-processar-lote btn-lote" title="Editar">Editar</button>
        <button data-action="excluir" data-id="${s.id}" class="btn-excluir-lote btn-lote" title="Excluir">Excluir</button>
      </td>
    </tr>
    <tr class="setor-detail-row" data-setor-id="${s.id}" style="display: none;">
      <td colspan="5">
        <div class="setor-tipos-card">
          ${temTipos ? tiposTexto : '<span class="tipos-vazio">Nenhum tipo vinculado a este setor</span>'}
        </div>
      </td>
    </tr>
  `;
}

btnNovoSetor.addEventListener("click", async () => {
  setorId.value = "";
  setorNome.value = "";
  setorEmail.value = "";
  setorWhatsapp.value = "";
  setorStatus.value = "ATIVO";
  modalSetorTitulo.textContent = "Novo setor";
  modalSetor.style.display = "flex";
  tiposCheckboxes.innerHTML = '<span class="form-hint">Carregando tipos...</span>';
  try {
    const tipos = await loadTipos();
    renderTiposCheckboxes(tipos, []);
  } catch (_) {
    tiposCheckboxes.innerHTML = '<p class="form-hint">Erro ao carregar tipos.</p>';
  }
  setorNome.focus();
});

modalSetorCancelar.addEventListener("click", () => {
  modalSetor.style.display = "none";
});

modalSetor.addEventListener("click", (e) => {
  if (e.target === modalSetor) modalSetorCancelar.click();
});

async function loadTipos() {
  const r = await authFetch("/api/tipos");
  const data = await r.json();
  if (!r.ok) throw new Error(data.detail || "Erro ao carregar tipos");
  return data;
}

function renderTiposCheckboxes(tipos, selectedIds) {
  const ids = new Set((selectedIds || []).map(Number));
  tiposCheckboxes.innerHTML = tipos.length
    ? tipos.map(t => `
        <label class="tipo-checkbox">
          <input type="checkbox" value="${t.id}" ${ids.has(Number(t.id)) ? "checked" : ""}>
          <span>${escapeHtml(t.tip_nome)}</span>
        </label>
      `).join("")
    : '<p class="form-hint">Nenhum tipo cadastrado. Cadastre tipos em Tipos de Ocorrência.</p>';
}

function getTiposSelecionados() {
  return Array.from(tiposCheckboxes.querySelectorAll("input:checked")).map(el => parseInt(el.value, 10));
}

async function abrirEditar(id) {
  showLoad("Carregando...");
  try {
    const [rSetor, tipos] = await Promise.all([
      authFetch(`/api/setores/${id}`).then(r => r.json()),
      loadTipos(),
    ]);
    const s = rSetor;
    setorId.value = s.id;
    setorNome.value = s.set_nome || "";
    setorEmail.value = s.set_email || "";
    setorWhatsapp.value = s.set_whatsapp || "";
    setorStatus.value = s.set_status === "INATIVO" ? "INATIVO" : "ATIVO";
    renderTiposCheckboxes(tipos, s.tip_ids || []);
    modalSetorTitulo.textContent = "Editar setor";
    modalSetor.style.display = "flex";
    setorNome.focus();
  } catch (e) {
    showToast(e.message || "Erro ao carregar setor", "error");
  } finally {
    hideLoad();
  }
}

modalSetorSalvar.addEventListener("click", async () => {
  const nome = setorNome.value.trim();
  const email = setorEmail.value.trim();
  const whatsapp = setorWhatsapp.value.trim();
  if (!nome || !email) {
    showToast("Nome e e-mail são obrigatórios", "error");
    return;
  }
  const id = setorId.value;
  const isNovo = !id;
  modalSetorSalvar.disabled = true;
  showLoad(isNovo ? "Salvando..." : "Atualizando...");
  try {
    const body = {
      set_nome: nome,
      set_email: email,
      set_whatsapp: whatsapp || null,
      set_status: setorStatus.value || "ATIVO",
      tip_ids: getTiposSelecionados(),
    };
    const url = isNovo ? "/api/setores" : `/api/setores/${id}`;
    const method = isNovo ? "POST" : "PUT";
    const r = await authFetch(url, {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || data.msg || "Erro ao salvar");
    showToast(isNovo ? "Setor criado" : "Setor atualizado", "success");
    modalSetor.style.display = "none";
    loadSetores();
  } catch (e) {
    showToast(e.message || "Erro ao salvar", "error");
  } finally {
    hideLoad();
    modalSetorSalvar.disabled = false;
  }
});

async function excluirSetor(id, nome) {
  if (!confirm(`Excluir o setor "${escapeHtml(nome)}"? Os vínculos com tipos serão removidos.`)) return;
  showLoad("Excluindo...");
  try {
    const r = await authFetch(`/api/setores/${id}`, { method: "DELETE" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || "Erro ao excluir");
    showToast("Setor excluído", "success");
    loadSetores();
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
loadSetores();
