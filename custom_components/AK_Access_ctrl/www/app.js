// Minimal helper to call HA API using the frontend's stored token.
function getAccessToken() {
  try {
    const raw = localStorage.getItem("hassTokens");
    if (!raw) return null;
    const obj = JSON.parse(raw);
    return obj?.access_token || null;
  } catch (e) {
    return null;
  }
}
const authHeader = () => ({ "Authorization": "Bearer " + getAccessToken() });

async function haGet(path) {
  const r = await fetch(path, { headers: authHeader() });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}
async function haPost(path, data = {}) {
  const r = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeader() },
    body: JSON.stringify(data),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json().catch(() => ({})); // services often return no body
}

// ---- UI helpers
function badge(text, cls) { return `<span class="badge ${cls}">${text}</span>`; }
function esc(s) { return (""+s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

async function loadDevices() {
  // We read entity states and infer per-device health from your integration's sensors.
  // Any entity exposing akuvox_entry_id + akuvox_metric attributes will be grouped together.
  const states = await haGet("/api/states");
  const devs = {};
  for (const s of states) {
    const attrs = s.attributes || {};
    const key = String(attrs.akuvox_entry_id || "").trim();
    if (!key) continue;
    if (!devs[key]) {
      devs[key] = {
        key,
        name: attrs.akuvox_device_name || attrs.friendly_name || key,
        type: attrs.akuvox_device_type || "",
        entities: {},
        metrics: {},
      };
    }
    devs[key].entities[s.entity_id] = s;
    const metric = String(attrs.akuvox_metric || "").trim();
    if (metric) {
      devs[key].metrics[metric] = s;
    }
  }

  const holder = document.getElementById("devices");
  holder.innerHTML = "";
  const items = Object.values(devs).sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
  for (const d of items) {
    let onlineEntity = d.metrics.online;
    let syncEntity = d.metrics.sync_status;
    let lastEntity = d.metrics.last_sync;

    if (!onlineEntity || !syncEntity || !lastEntity) {
      const allEntities = Object.values(d.entities);
      onlineEntity ||= allEntities.find(x => x.entity_id.includes("_online"));
      syncEntity ||= allEntities.find(x => x.entity_id.includes("_sync_status"));
      lastEntity ||= allEntities.find(x => x.entity_id.includes("_last_sync"));
    }

    const onlineState = String(onlineEntity?.state || "").toLowerCase();
    const online = ["online", "on", "true"].includes(onlineState);
    const syncState = syncEntity?.state || "unknown";
    const lastSync = lastEntity?.state || "";

    const onlineBadge = online ? badge("Online", "badge-online") : badge("Offline", "badge-offline");
    const syncBadge = syncState === "in_sync"
      ? badge("In Sync", "badge-in-sync")
      : syncState === "in_progress"
      ? badge("In Progress", "badge-in-progress bg-info text-dark")
      : syncState === "pending"
      ? badge("Pending Sync", "badge-pending")
      : badge(esc(syncState), "bg-secondary");

    const card = document.createElement("div");
    card.className = "col-12 col-md-6 col-xl-4";
    card.innerHTML = `
      <div class="p-3 border rounded h-100">
        <div class="d-flex justify-content-between align-items-start">
          <div>
            <div class="fw-semibold">${esc(d.name)}</div>
            <div class="mt-1">${onlineBadge} ${syncBadge}</div>
          </div>
          <div class="text-end">
            <button class="btn btn-sm btn-outline-light me-1" data-act="syncNowOne" data-key="${esc(d.key)}">Sync Now</button>
            <button class="btn btn-sm btn-outline-light me-1" data-act="forceOne" data-key="${esc(d.key)}">Force Sync</button>
            <button class="btn btn-sm btn-outline-light" data-act="rebootOne" data-key="${esc(d.key)}">Reboot</button>
          </div>
        </div>
        <div class="small-mono mt-2">Last sync: ${esc(lastSync || "—")}</div>
      </div>`;
    holder.appendChild(card);
  }

  // wire buttons: per-device force/sync/reboot are routed via the integration's "all" services for simplicity
  holder.querySelectorAll("button[data-act='syncNowOne']").forEach(b => b.addEventListener("click", () => haPost("/api/services/akuvox_ac/sync_now")));
  holder.querySelectorAll("button[data-act='forceOne']").forEach(b => b.addEventListener("click", () => haPost("/api/services/akuvox_ac/force_full_sync")));
  holder.querySelectorAll("button[data-act='rebootOne']").forEach(b => b.addEventListener("click", () => haPost("/api/services/akuvox_ac/reboot_device")));
}

async function loadEvents() {
  // Expect event list sensors or a combined event entity exposed by your integration.
  // If not present, we just trigger a refresh and render whatever text sensor you expose.
  await haPost("/api/services/akuvox_ac/refresh_events");
  const states = await haGet("/api/states");
  const eventSensors = states.filter(s => s.entity_id.startsWith("sensor.akuvox_") && s.attributes?.device_class === "timestamp" && s.attributes?.akuvox_event);
  const box = document.getElementById("events");
  box.innerHTML = "";
  const items = [];
  for (const s of states) {
    if (s.attributes?.akuvox_event_list) {
      // If your integration exposes a JSON array in attributes.akuvox_event_list – use it.
      try {
        const arr = s.attributes.akuvox_event_list;
        for (const e of arr) {
          items.push(e);
        }
      } catch (e) {}
    }
  }
  // Fallback: nothing special exposed – show a friendly note.
  if (!items.length) {
    box.innerHTML = `<div class="text-muted">Events loaded. Expose an event list sensor via the integration to show them here.</div>`;
    return;
  }
  // Sort newest first
  items.sort((a,b) => (new Date(b.ts||b.time||0)) - (new Date(a.ts||a.time||0)));
  items.slice(0, 50).forEach(e => {
    const line = document.createElement("div");
    line.textContent = `[${e.ts||e.time}] ${e.device||""} • ${e.user||""} • ${e.event||e.type||""} (${e.method||""})`;
    box.appendChild(line);
  });
}

async function loadUsers() {
  // We’ll read HA state list and show two sets:
  //  - HA users (entity_id like sensor.akuvox_user_HA-001) – editable
  //  - Cloud users (sensor.akuvox_cloud_user_*) – read-only
  // If you don’t have such sensors, this will fall back to a note.
  const states = await haGet("/api/states");
  const usersDiv = document.getElementById("users");
  usersDiv.innerHTML = "";

  const haUsers = [];
  const cloudUsers = [];
  for (const s of states) {
    if (!s.entity_id.startsWith("sensor.akuvox_user_")) continue;
    const id = s.attributes?.user_id || s.attributes?.UserID || s.entity_id.replace("sensor.akuvox_user_", "");
    const name = s.attributes?.name || s.attributes?.Name || s.state || id;
    const source = (s.attributes?.source || "").toLowerCase();
    const groups = s.attributes?.groups || [];
    const last_access = s.attributes?.last_access || "";
    const item = { id, name, source, groups, last_access };
    if (id?.startsWith("HA-")) haUsers.push(item);
    else if (source === "cloud") cloudUsers.push(item);
  }

  function renderUser(u, editable) {
    const wrap = document.createElement("div");
    wrap.className = "border-bottom py-2";
    wrap.innerHTML = `
      <div class="d-flex justify-content-between align-items-center">
        <div>
          <div class="fw-semibold">${esc(u.name)} <span class="small-mono text-muted">(${esc(u.id)})</span></div>
          <div class="small text-muted">Groups: ${esc((u.groups||[]).join(", ") || "—")} • Last access: ${esc(u.last_access||"—")}</div>
        </div>
        <div>
          ${editable ? `
            <button class="btn btn-sm btn-outline-light me-2" data-edit="${esc(u.id)}">Set Groups</button>
            <button class="btn btn-sm btn-outline-light me-2" data-face="${esc(u.id)}">Upload Face</button>
            <button class="btn btn-sm btn-danger" data-del="${esc(u.id)}">Delete</button>
          ` : `<span class="badge bg-secondary">Cloud (read-only)</span>`}
        </div>
      </div>`;
    usersDiv.appendChild(wrap);
  }

  if (!haUsers.length && !cloudUsers.length) {
    usersDiv.innerHTML = `<div class="text-muted">Expose per-user sensors (e.g. sensor.akuvox_user_HA-001) to list users here. You can still use the form above to add users.</div>`;
    return;
  }

  haUsers.sort((a,b)=>a.name.localeCompare(b.name)).forEach(u => renderUser(u, true));
  cloudUsers.sort((a,b)=>a.name.localeCompare(b.name)).forEach(u => renderUser(u, false));

  // Wire actions
  usersDiv.querySelectorAll("button[data-del]").forEach(btn => {
    btn.addEventListener("click", async () => {
      if (!confirm("Delete user " + btn.dataset.del + "?")) return;
      await haPost("/api/services/akuvox_ac/delete_user", { id: btn.dataset.del });
      await loadUsers();
    });
  });
  usersDiv.querySelectorAll("button[data-face]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const id = btn.dataset.face;
      const path = prompt("Full image path under /config/www (e.g. /config/www/faces/steve.jpg):");
      if (!path) return;
      await haPost("/api/services/akuvox_ac/upload_face", { id, face_image_path: path });
      alert("Face queued. Device(s) marked pending sync.");
    });
  });
  usersDiv.querySelectorAll("button[data-edit]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const id = btn.dataset.edit;
      const groupsStr = prompt("Enter groups (comma separated):", "Default");
      if (groupsStr === null) return;
      const groups = groupsStr.split(",").map(s => s.trim()).filter(Boolean);
      await haPost("/api/services/akuvox_ac/set_user_groups", { key: id, groups });
      await loadDevices(); // permissions applied immediately; refresh statuses
      await loadUsers();
    });
  });
}

// ---- Add User form
document.getElementById("addUserForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const name = fd.get("name");
  const pin = fd.get("pin");
  const card = fd.get("card_code");
  const groups = (fd.get("groups") || "").toString().split(",").map(s => s.trim()).filter(Boolean);
  const face = fd.get("face_image_path");
  try {
    await haPost("/api/services/akuvox_ac/add_user", {
      name, pin, card_code: card,
      sync_groups: groups.length ? groups : ["Default"],
      face_image_path: face || undefined
    });
    e.target.reset();
    await loadDevices(); // devices now pending
    await loadUsers();
  } catch (err) {
    alert("Error adding user: " + err);
  }
});

// ---- Top buttons
document.getElementById("refreshAll").addEventListener("click", async () => {
  await loadDevices(); await loadUsers(); await loadEvents();
});
document.getElementById("syncNow").addEventListener("click", async () => {
  await haPost("/api/services/akuvox_ac/sync_now", {});
  setTimeout(loadDevices, 1500);
});
document.getElementById("forceFull").addEventListener("click", async () => {
  await haPost("/api/services/akuvox_ac/force_full_sync", {});
  setTimeout(loadDevices, 1500);
});
document.getElementById("reboot").addEventListener("click", async () => {
  if (!confirm("Reboot first Akuvox device?")) return;
  await haPost("/api/services/akuvox_ac/reboot_device", {});
});

document.getElementById("refreshEvents").addEventListener("click", async () => {
  await loadEvents();
});

// Initial load
(async function init() {
  try {
    await loadDevices();
    await loadUsers();
    await loadEvents();
  } catch (e) {
    console.error(e);
    alert("Failed to load. Open this panel from inside Home Assistant so it can use your session.");
  }
})();
