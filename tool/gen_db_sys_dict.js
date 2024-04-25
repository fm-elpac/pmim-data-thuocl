#!/usr/bin/env -S deno run -A --unstable-kv
// pmim-data-thuocl/tool/gen_db_sys_dict.js
// 生成 pmim_sys.db 数据库 (词库)
//
// 命令行示例:
// > deno run -A --unstable-kv gen_db_sys_dict.js pmim_sys.db thuocl
import { join } from "https://deno.land/std@0.223.0/path/join.ts";

import { batch_set, chunk_get } from "./kv_util.js";

// thuocl/*.txt
async function 加载数据文件(目录) {
  const 结果 = [];

  console.log("加载数据文件目录: " + 目录);
  for await (const i of Deno.readDir(目录)) {
    if (i.name.endsWith(".txt")) {
      const 路径 = join(目录, i.name);
      console.log("加载: " + 路径);

      // 加载单个文件
      const 文本 = await Deno.readTextFile(路径);
      const 行 = 文本.split("\n");
      for (const j of 行) {
        const 内容 = j.trim();
        // 忽略空行
        if (内容.length < 1) {
          continue;
        }

        // 每行数据有 2 列: 词 频率
        // 之间以 制表符 (tab) 分隔
        const p = 内容.split("	");
        if (p.length < 2) {
          console.log("错误格式数据: " + 内容);
          continue;
        }
        const 词 = p[0].trim();
        const 频率1 = p[1].trim();
        // 忽略格式不正确的行
        if ((词.length < 1) || (频率1.length < 1)) {
          console.log("错误格式数据: " + 内容);
          continue;
        }
        const 频率 = Number.parseInt(频率1);
        if (Number.isNaN(频率)) {
          console.log("错误格式数据: " + 内容);
          continue;
        }

        // 保存结果
        结果.push([词, 频率]);
      }
    }
  }
  return 结果;
}

class 拼音读取器 {
  constructor(kv) {
    this.kv = kv;
    this.cache = {};
  }

  async 初始化() {
    // 加载 preload/pinyin_tgh
    this.pt = await chunk_get(this.kv, ["data", "preload", "pinyin_tgh"]);
  }

  // 获取汉字对应的拼音
  async 拼音(c) {
    if (this.pt.cp[c] != null) {
      return this.pt.cp[c];
    }

    if (this.cache[c] != null) {
      return this.cache[c];
    }

    const { value } = await this.kv.get(["data", "pinyin", c]);
    if (value != null) {
      this.cache[c] = value;
      return value;
    }
    // 无法获取拼音
    return null;
  }
}

// 将字符串按照 unicode code point 切分成单个字符
export function u切分(s) {
  const o = [];
  let i = 0;
  while (i < s.length) {
    const c = s.codePointAt(i);
    o.push(String.fromCodePoint(c));
    if (c > 0xffff) {
      i += 2;
    } else {
      i += 1;
    }
  }
  return o;
}

async function 处理(kv, 数据, p) {
  console.log("处理()  " + 数据.length);

  let 词数 = 0;

  const 写入 = [];
  // 收集所有前缀
  const pt = {};
  // 拼音至前缀
  const pp = {};
  for (const [词1, 频率] of 数据) {
    const 词 = u切分(词1);
    // 词至少是 2 个字
    if (词.length < 2) {
      continue;
    }
    // 前缀是词的前 2 个字
    const 前缀 = 词.slice(0, 2).join("");
    // 获取前缀的拼音
    const p1 = await p.拼音(词[0]);
    const p2 = await p.拼音(词[1]);
    if ((null == p1) || (null == p2)) {
      console.log("忽略词 (无拼音): " + 词1);
      continue;
    }

    // 频率
    写入.push([["data", "dict", 前缀, 词1], 频率]);
    词数 += 1;
    // 收集前缀
    if (pt[前缀] != null) {
      pt[前缀].push(词1);
    } else {
      pt[前缀] = [词1];
    }

    // 生成拼音至前缀
    // TODO 正确处理 多音字 ?
    for (const i of p1) {
      for (const j of p2) {
        const pin_yin = i + "_" + j;
        if (pp[pin_yin] != null) {
          pp[pin_yin].push(前缀);
        } else {
          pp[pin_yin] = [前缀];
        }
      }
    }
  }
  // DEBUG
  console.log("  词数: " + 词数);
  console.log("  前缀 -> 词: " + 写入.length);
  console.log("  前缀 " + Object.keys(pt).length);
  console.log("  拼音 -> 前缀 " + Object.keys(pp).length);
  // 保存前缀
  for (const i of Object.keys(pt)) {
    写入.push([["data", "dict", i], pt[i]]);
  }
  // 保存拼音
  for (const i of Object.keys(pp)) {
    写入.push([["data", "dict", i], pp[i]]);
  }
  await batch_set(kv, 写入, 1000);

  // 元数据
  console.log("写入元数据");
  const PMIM_DB_VERSION = "pmim_sys_db version 0.1.0";
  const PMIM_VERSION = "pmim version 0.1.5";

  await kv.set(["pmim_db", "v"], {
    pmim: PMIM_VERSION,
    deno_version: Deno.version,
    n: "胖喵拼音内置数据库 (10 万词, THUOCL)",
    _last_update: new Date().toISOString(),
  });
}

async function main() {
  const 输出 = Deno.args[0];
  console.log(`${输出}`);

  const 目录 = Deno.args[1];
  // 读取数据
  const 数据 = await 加载数据文件(目录);

  // 打开数据库
  const kv = await Deno.openKv(输出);

  const p = new 拼音读取器(kv);
  await p.初始化();
  await 处理(kv, 数据, p);

  // 记得关闭数据库
  kv.close();
}

if (import.meta.main) main();
