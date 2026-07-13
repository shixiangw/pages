#!/usr/bin/env node
// 生成本仓库的静态文件清单 filelist.json，替代前端直接调用 GitHub API（避免 60 次/小时限流）。
// 用法：node generate-filelist.js
// 在 Cloudflare Pages 中可将「构建命令」设为 `node generate-filelist.js`，每次部署自动刷新清单。

const fs = require('fs');
const path = require('path');

const ROOT = __dirname;
const OUTPUT = path.join(ROOT, 'filelist.json');
const EXTENSIONS = new Set(['.html', '.htm', '.md', '.markdown']);

function getExt(name) {
  const idx = name.lastIndexOf('.');
  return idx === -1 ? '' : name.slice(idx).toLowerCase();
}

function walk(dir, base, acc) {
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return acc;
  }
  for (const entry of entries) {
    const abs = path.join(dir, entry.name);
    const rel = base ? `${base}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      // 跳过版本库 / 隐藏目录 / 工作区元数据
      if (entry.name === '.git' || entry.name === '.workbuddy' || entry.name.startsWith('.')) continue;
      walk(abs, rel, acc);
    } else if (entry.isFile()) {
      if (entry.name === 'filelist.json') continue; // 不要把清单本身列进去
      const ext = getExt(entry.name);
      if (!EXTENSIONS.has(ext)) continue;
      let size = 0;
      try { size = fs.statSync(abs).size; } catch { /* ignore */ }
      acc.push({ path: rel, size, type: 'blob' });
    }
  }
  return acc;
}

const tree = walk(ROOT, '', []).sort((a, b) => a.path.localeCompare(b.path));

const out = {
  generatedAt: new Date().toISOString(),
  tree,
};

fs.writeFileSync(OUTPUT, JSON.stringify(out, null, 2) + '\n', 'utf8');
console.log(`✅ 生成 filelist.json，共 ${tree.length} 个文件`);
