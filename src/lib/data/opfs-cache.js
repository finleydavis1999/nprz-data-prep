// Versioned OPFS cache for parquet files.
//
// Layout in the OPFS root:
//   <manifestVersion>/parquet/<dataset>-<scale>.parquet
//
// On init, any sibling versioned directory is removed — the manifest version
// is the cache key, so a new build invalidates the entire previous cache.

let initPromise = null;

// Returns the FileSystemDirectoryHandle for the current version's namespace.
// Idempotent — subsequent calls return the same handle.
export async function initCache(manifestVersion) {
	if (!initPromise) initPromise = doInit(manifestVersion);
	return initPromise;
}

async function doInit(manifestVersion) {
	const root = await navigator.storage.getDirectory();
	for await (const entry of root.values()) {
		if (entry.kind === 'directory' && entry.name !== manifestVersion) {
			await root.removeEntry(entry.name, { recursive: true });
		}
	}
	return root.getDirectoryHandle(manifestVersion, { create: true });
}

// Get a FileSystemFileHandle for `relativePath`, fetching from `srcUrl` on miss.
// `relativePath` may contain `/` separators — intermediate dirs are created.
export async function getOrFetch(versionRoot, relativePath, srcUrl) {
	const parts = relativePath.split('/');
	const filename = parts.pop();
	let dir = versionRoot;
	for (const p of parts) {
		dir = await dir.getDirectoryHandle(p, { create: true });
	}
	try {
		const handle = await dir.getFileHandle(filename);
		const file = await handle.getFile();
		if (file.size > 0) return handle;
		// Zero-byte file from a previous failed write — re-fetch.
	} catch {
		// Falls through to fetch.
	}
	const handle = await dir.getFileHandle(filename, { create: true });
	const res = await fetch(srcUrl);
	if (!res.ok || !res.body) throw new Error(`fetch ${srcUrl}: HTTP ${res.status}`);
	const writable = await handle.createWritable();
	await res.body.pipeTo(writable);
	return handle;
}
