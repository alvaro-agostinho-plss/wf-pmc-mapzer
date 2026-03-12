const base = () => (window.BASE_PATH || "");
if (!getToken()) {
  window.location.href = base() + "/login";
  throw new Error("Redirecionando para login");
}

const lotesContainer = document.getElementById("lotesContainer");
const modalIncluirLote = document.getElementById("modalIncluirLote");
const modalDropOcorr = document.getElementById("modalDropOcorr");
const modalDropOS = document.getElementById("modalDropOS");
const modalFileOcorr = document.getElementById("modalFileOcorr");
const modalFileOS = document.getElementById("modalFileOS");
const modalOcorrNome = document.getElementById("modalOcorrNome");
const modalOSNome = document.getElementById("modalOSNome");
const modalSalvar = document.getElementById("modalSalvar");
const modalCancelar = document.getElementById("modalCancelar");
const btnIncluirLote = document.getElementById("btnIncluirLote");

let modalUploadIds = { ocorrencias: null, os: null };

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

function setupModalDrop(el, inputEl, tipo, nomeEl) {
  el.addEventListener("click", () => inputEl.click());
  el.addEventListener("dragover", (e) => { e.preventDefault(); el.classList.add("dragover"); });
  el.addEventListener("dragleave", () => el.classList.remove("dragover"));
  el.addEventListener("drop", (e) => {
    e.preventDefault();
    el.classList.remove("dragover");
    if (e.dataTransfer.files.length) handleModalFile(e.dataTransfer.files[0], tipo, nomeEl);
  });
}
async function handleModalFile(file, tipoEsperado, nomeEl) {
  if (!file.name.toLowerCase().endsWith(".xlsx") && !file.name.toLowerCase().endsWith(".xls")) {
    showToast("Use arquivo .xlsx", "error");
    return;
  }
  showLoad("Enviando arquivo...");
  try {
    const formData = new FormData();
    formData.append("arquivo", file);
    const url = `/api/upload?tipo_esperado=${encodeURIComponent(tipoEsperado)}`;
    const r = await authFetch(url, { method: "POST", body: formData });
    const data = await r.json();
    if (!data.id) throw new Error(data.detail || "Erro no upload");
    modalUploadIds[tipoEsperado === "ocorrencias" ? "ocorrencias" : "os"] = data.id;
    nomeEl.textContent = data.nome || file.name;
    modalSalvar.disabled = !(modalUploadIds.ocorrencias && modalUploadIds.os);
    showToast(`Upload: ${data.nome}`, "success");
  } catch (e) {
    showToast(e.message || "Erro no upload", "error");
  } finally {
    hideLoad();
  }
}
modalFileOcorr.addEventListener("change", () => {
  if (modalFileOcorr.files.length) handleModalFile(modalFileOcorr.files[0], "ocorrencias", modalOcorrNome);
  modalFileOcorr.value = "";
});
modalFileOS.addEventListener("change", () => {
  if (modalFileOS.files.length) handleModalFile(modalFileOS.files[0], "os", modalOSNome);
  modalFileOS.value = "";
});

setupModalDrop(modalDropOcorr, modalFileOcorr, "ocorrencias", modalOcorrNome);
setupModalDrop(modalDropOS, modalFileOS, "os", modalOSNome);

btnIncluirLote.addEventListener("click", () => {
  modalUploadIds = { ocorrencias: null, os: null };
  modalOcorrNome.textContent = "";
  modalOSNome.textContent = "";
  modalSalvar.disabled = true;
  modalIncluirLote.style.display = "flex";
});

modalCancelar.addEventListener("click", () => {
  const ids = [modalUploadIds.ocorrencias, modalUploadIds.os].filter(Boolean);
  ids.forEach(id => authFetch(`/api/uploads/${id}`, { method: "DELETE" }).catch(() => {}));
  modalIncluirLote.style.display = "none";
});

modalSalvar.addEventListener("click", async () => {
  if (!modalUploadIds.ocorrencias || !modalUploadIds.os) return;
  modalSalvar.disabled = true;
  showLoad("Salvando envio...");
  try {
    const r = await authFetch("/api/lotes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        upl_id_ocorrencias: modalUploadIds.ocorrencias,
        upl_id_os: modalUploadIds.os,
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || data.msg || "Erro ao criar envio");
    showToast("Envio cadastrado", "success");
    modalIncluirLote.style.display = "none";
    loadLotes();
  } catch (e) {
    showToast(e.message || "Erro ao criar envio", "error");
    modalSalvar.disabled = false;
  } finally {
    hideLoad();
  }
});

modalIncluirLote.addEventListener("click", (e) => {
  if (e.target === modalIncluirLote) modalCancelar.click();
});

async function loadLotes() {
  try {
    const r = await authFetch("/api/lotes");
    const items = await r.json();
    if (!items.length) {
      lotesContainer.innerHTML = '<p class="empty">Nenhum envio cadastrado. Clique em "Incluir envio" para começar.</p>';
      lotesContainer.classList.remove("lotes-table");
    } else {
      lotesContainer.classList.add("lotes-table");
      lotesContainer.innerHTML = `
        <table class="lotes-grid">
          <colgroup>
            <col class="col-ocorr">
            <col class="col-os">
            <col class="col-data">
            <col class="col-data">
            <col class="col-acoes">
          </colgroup>
          <thead>
            <tr>
              <th>Ocorrência</th>
              <th>OS</th>
              <th>Processamento</th>
              <th>E-mail</th>
              <th class="th-acoes">Ações</th>
            </tr>
          </thead>
          <tbody>
            ${items.map(l => renderLoteRow(l)).join("")}
          </tbody>
        </table>
      `;
      items.forEach(l => {
        document.querySelector(`[data-action="processar"][data-id="${l.id}"]`)?.addEventListener("click", () => processarLote(l.id));
        document.querySelector(`[data-action="enviar"][data-id="${l.id}"]`)?.addEventListener("click", () => enviarEmailLote(l.id));
        document.querySelector(`[data-action="excluir"][data-id="${l.id}"]`)?.addEventListener("click", () => excluirLote(l.id));
      });
      lotesContainer.querySelectorAll(".btn-download").forEach(link => {
        link.addEventListener("click", (e) => {
          e.preventDefault();
          downloadArquivo(link.dataset.uploadId, link.dataset.filename);
        });
      });
    }
  } catch (e) {
    lotesContainer.innerHTML = `<p class="empty">Erro: ${e.message}</p>`;
    lotesContainer.classList.remove("lotes-table");
  }
}

function renderLoteRow(l) {
  const processado = !!l.data_processamento;
  const emailEnviado = !!l.data_envio_email;
  const dtProc = l.data_processamento ? new Date(l.data_processamento).toLocaleString("pt-BR", { day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit" }) : "—";
  const dtEmail = l.data_envio_email ? new Date(l.data_envio_email).toLocaleString("pt-BR", { day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit" }) : "—";
  const textoBtnEmail = (processado && emailEnviado) ? "Reenviar" : "Enviar e-mail";
  const cellFile = (uplId, nome, nomeClass) => {
    const txt = escapeHtml(nome || "—");
    if (!uplId || !nome) return `<span class="${nomeClass}">${txt}</span>`;
    return `<span class="cell-file"><span class="${nomeClass}">${txt}</span><a href="#" class="btn-download" data-upload-id="${uplId}" data-filename="${escapeHtml(nome)}" title="Baixar planilha"><i class="fa-solid fa-download"></i></a></span>`;
  };
  return `
    <tr class="lote-row" data-lote-id="${l.id}">
      <td class="col-ocorr">${cellFile(l.upl_id_ocorrencias, l.nome_ocorrencias, "nome-ocorr")}</td>
      <td class="col-os">${cellFile(l.upl_id_os, l.nome_os, "nome-os")}</td>
      <td class="col-data">${dtProc}</td>
      <td class="col-data">${dtEmail}</td>
      <td class="col-acoes">
        <button data-action="processar" data-id="${l.id}" class="btn-processar-lote btn-lote" ${processado ? "disabled" : ""} title="Processar">Processar</button>
        <button data-action="enviar" data-id="${l.id}" class="btn-email btn-lote" ${processado ? "" : "disabled"} title="${textoBtnEmail}">${textoBtnEmail}</button>
        <button data-action="excluir" data-id="${l.id}" class="btn-excluir-lote btn-lote" title="Excluir">Excluir</button>
      </td>
    </tr>
  `;
}

async function downloadArquivo(uploadId, filename) {
  try {
    const r = await authFetch("api/uploads/" + uploadId + "/download");
    if (!r.ok) throw new Error("Erro ao baixar");
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename || "planilha.xlsx";
    a.click();
    URL.revokeObjectURL(url);
  } catch (e) {
    showToast(e.message || "Erro ao baixar arquivo", "error");
  }
}

async function processarLote(id) {
  const btn = document.querySelector(`[data-action="processar"][data-id="${id}"]`);
  if (btn?.disabled) return;
  if (btn) btn.disabled = true;
  showLoad("Processando ocorrências e OS...");
  try {
    const r = await authFetch(`/api/lotes/${id}/processar`, { method: "POST" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(Array.isArray(data.detail) ? data.detail.join(" ") : data.detail || "Erro");
    showToast(`${data.total_ocorrencias ?? 0} ocorrências + ${data.total_os ?? 0} OS`, "success");
    loadLotes();
  } catch (e) {
    showToast(e.message || "Erro ao processar", "error");
    loadLotes();
  } finally {
    hideLoad();
  }
}

async function enviarEmailLote(id) {
  const btn = document.querySelector(`[data-action="enviar"][data-id="${id}"]`);
  if (btn?.disabled) return;
  if (btn) btn.disabled = true;
  showLoad("Enviando e-mails...");
  try {
    const r = await authFetch(`/api/lotes/${id}/enviar-email`, { method: "POST" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(Array.isArray(data.detail) ? data.detail.join(" ") : data.detail || "Erro");
    const total = Object.values(data).filter(Boolean).length;
    showToast(`E-mails enviados (${total} destinatários)`, "success");
    loadLotes();
  } catch (e) {
    showToast(e.message || "Erro ao enviar", "error");
    loadLotes();
  } finally {
    hideLoad();
  }
}

async function excluirLote(id) {
  if (!confirm("Excluir este workflow? Os dois arquivos serão removidos.")) return;
  showLoad("Excluindo...");
  try {
    const r = await authFetch(`/api/lotes/${id}`, { method: "DELETE" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || "Erro ao excluir");
    showToast("Envio excluído", "success");
    loadLotes();
  } catch (e) {
    showToast(e.message || "Erro ao excluir envio", "error");
    loadLotes();
  } finally {
    hideLoad();
  }
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
loadLotes();
