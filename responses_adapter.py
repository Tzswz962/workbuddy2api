"""
responses_adapter.py — OpenAI Responses API ↔ Chat Completions API 适配层。

Codex CLI 使用 Responses API（POST /v1/responses），而 CodeBuddy 后端只支持
Chat Completions 协议。本模块做双向转换：
  请求：Responses input/instructions/tools → Chat messages/tools
  响应：Chat SSE delta → Responses 语义事件流（response.created / output_text.delta / …）

事件类型参考：https://developers.openai.com/api/docs/guides/streaming-responses
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

# ---------------------------------------------------------------------------
# 客户端事件契约：只发「Responses 规范定义、且客户端真正接受」的事件
# ---------------------------------------------------------------------------
# OpenAI Responses 规范里，若干事件的字段是**必填**的（见下）。主流客户端用严格
# schema 校验每个 SSE 事件，校验不通过时**逐条丢弃**该事件（而不是中断整条流），
# 所以故障表现为静默丢数据，而不是报错。
#
# 两个真实踩坑点，症状都是「思考完就断、没有正文」：
#   1) response.output_text.delta 少了必填的 item_id → 每一条正文 delta 都被
#      丢掉；reasoning 事件字段齐全所以思考能正常显示，最终只剩空回复。
#   2) response.in_progress / response.content_part.* / response.output_text.done /
#      response.reasoning_summary_text.done / response.function_call_arguments.done
#      属于可选扩展、并非所有客户端都实现 → 发了会被判为非法，白白污染事件流。
#
# 所以这里按「官方 Responses 规范的必需事件子集」发最小事件集：字段齐全（合规），
# 且不依赖可选扩展事件（兼容性最好）。
ALLOWED_RESPONSES_EVENTS = frozenset({
    "response.created",
    "response.output_item.added",
    "response.output_item.done",
    "response.output_text.delta",
    "response.reasoning_summary_part.added",
    "response.reasoning_summary_text.delta",
    "response.reasoning_summary_part.done",
    "response.function_call_arguments.delta",
    "response.completed",
    "response.incomplete",
    "response.failed",
})

# 部分客户端（老版本 Codex 等）会额外期待 content_part / *.done 这类收尾事件。
# 置 True 会恢复发送这些可选扩展事件；代价是严格校验的客户端会收到非法事件。
# 默认 False：只发 ALLOWED_RESPONSES_EVENTS 里的必需事件。
EMIT_EXTENDED_EVENTS = False

# 仅在 EMIT_EXTENDED_EVENTS=True 时才额外发送的可选扩展事件
EXTENDED_ONLY_EVENTS = frozenset({
    "response.in_progress",
    "response.content_part.added",
    "response.content_part.done",
    "response.output_text.done",
    "response.reasoning_summary_text.done",
    "response.function_call_arguments.done",
})

# ---------------------------------------------------------------------------
# ID 生成
# ---------------------------------------------------------------------------

def _rand_id(prefix: str = "resp_") -> str:
    return prefix + os.urandom(12).hex()

# ---------------------------------------------------------------------------
# 请求转换：Responses → Chat
# ---------------------------------------------------------------------------

def responses_request_to_chat(body: dict) -> dict:
    """将 Responses API 请求体转换为 Chat Completions 请求体。

    关键映射：
      input → messages
      instructions → system message（置顶）
      max_output_tokens → max_tokens
      tools 格式微调（Responses 用 name，Chat 用 function.name）
    """
    messages: list[dict] = []

    # instructions → system message
    instructions = body.get("instructions")
    if instructions:
        messages.append({"role": "system", "content": instructions})

    # input → messages
    inp = body.get("input", [])
    if isinstance(inp, str):
        messages.append({"role": "user", "content": inp})
    elif isinstance(inp, list):
        messages.extend(_convert_input_items(inp))

    # 构造 Chat body
    chat: dict[str, Any] = {"messages": messages, "stream": True}

    # model
    if "model" in body:
        chat["model"] = body["model"]

    # tools — Responses 和 Chat 的 function tool 格式略有不同
    tools = body.get("tools")
    if tools:
        chat["tools"] = _convert_tools_for_chat(tools)
    if "tool_choice" in body:
        chat["tool_choice"] = body["tool_choice"]

    # 透传常见参数
    for key in ("temperature", "top_p", "stop", "seed",
                "presence_penalty", "frequency_penalty",
                "response_format", "reasoning_effort"):
        if key in body:
            chat[key] = body[key]

    # reasoning → reasoning_effort
    # Responses API 用嵌套对象 reasoning:{effort, summary} 表达思考强度，
    # 而 Chat/上游只认扁平的 reasoning_effort。此前**完全没有映射**，
    # 导致走 Responses 的客户端（Codex 等）档位被静默丢弃、上游不吐思维链。
    reasoning = body.get("reasoning")
    if isinstance(reasoning, dict):
        effort = reasoning.get("effort")
        if isinstance(effort, str) and effort.strip():
            chat.setdefault("reasoning_effort", effort.strip())
    elif isinstance(reasoning, str) and reasoning.strip():
        # 少数客户端直接给字符串
        chat.setdefault("reasoning_effort", reasoning.strip())

    # max_output_tokens → max_tokens
    if "max_output_tokens" in body:
        chat["max_tokens"] = body["max_output_tokens"]
    elif "max_tokens" in body:
        chat["max_tokens"] = body["max_tokens"]

    return chat


def _convert_input_items(items: list) -> list[dict]:
    """将 Responses API 的 input 数组转换为 Chat messages。

    input 里可能包含：
      - {"role": "user/developer", "content": ...}   → 直接映射
      - {"type": "message", ...}                      → 助手消息
      - {"type": "function_call", ...}                → 需合并到前面的助手消息
      - {"type": "function_call_output", ...}         → tool 角色
    """
    messages: list[dict] = []
    # 临时缓存：合并相邻的 assistant message 和 function_call
    pending_assistant_content: str | None = None
    pending_tool_calls: list[dict] = []

    def _flush_assistant():
        nonlocal pending_assistant_content, pending_tool_calls
        if pending_assistant_content is not None or pending_tool_calls:
            msg: dict[str, Any] = {"role": "assistant",
                                   "content": pending_assistant_content or ""}
            if pending_tool_calls:
                msg["tool_calls"] = pending_tool_calls[:]
            messages.append(msg)
            pending_assistant_content = None
            pending_tool_calls.clear()

    for item in items:
        if not isinstance(item, dict):
            continue

        item_type = item.get("type")
        role = item.get("role", "")

        # 简单消息 {"role": "user", "content": "..."}
        if item_type is None and role in ("user", "system", "developer"):
            _flush_assistant()
            mapped_role = "system" if role == "developer" else role
            content = _extract_content(item.get("content", ""))
            messages.append({"role": mapped_role, "content": content})
            continue

        # typed message（Responses 里常见）
        if item_type == "message" and role in ("user", "system", "developer"):
            _flush_assistant()
            mapped_role = "system" if role == "developer" else role
            content = _extract_content(item.get("content", ""))
            messages.append({"role": mapped_role, "content": content})
            continue

        # assistant 消息（来自前一轮输出）
        if item_type == "message" and role == "assistant":
            _flush_assistant()
            content_parts = item.get("content", [])
            text = _extract_output_text(content_parts) if isinstance(content_parts, list) else str(content_parts)
            pending_assistant_content = text
            continue

        # 简单 role=assistant（无 type 标记）
        if item_type is None and role == "assistant":
            _flush_assistant()
            content = _extract_content(item.get("content", ""))
            pending_assistant_content = content
            continue

        # function_call — 合并到前面的 assistant 消息
        if item_type == "function_call":
            if pending_assistant_content is None:
                pending_assistant_content = ""
            pending_tool_calls.append({
                "id": item.get("call_id", item.get("id", _rand_id("call_"))),
                "type": "function",
                "function": {
                    "name": item.get("name", ""),
                    "arguments": item.get("arguments", "{}"),
                },
            })
            continue

        # function_call_output → tool 消息
        if item_type == "function_call_output":
            _flush_assistant()
            messages.append({
                "role": "tool",
                "tool_call_id": item.get("call_id", ""),
                "content": item.get("output", ""),
            })
            continue

        # 其他未知类型 — 尝试当作普通消息
        if role:
            _flush_assistant()
            content = _extract_content(item.get("content", ""))
            messages.append({"role": role, "content": content})

    _flush_assistant()
    return messages


def _extract_content(content) -> str:
    """提取 content（可能是 str / list[{type,text}]）。"""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, dict):
                if p.get("type") in ("input_text", "text"):
                    parts.append(p.get("text", ""))
                elif p.get("type") == "output_text":
                    parts.append(p.get("text", ""))
            elif isinstance(p, str):
                parts.append(p)
        return "".join(parts) or str(content)
    return str(content)


def _extract_output_text(content_parts: list) -> str:
    """从 Responses output content parts 提取纯文本。"""
    texts = []
    for part in content_parts:
        if isinstance(part, dict) and part.get("type") == "output_text":
            texts.append(part.get("text", ""))
    return "".join(texts)


def _convert_tools_for_chat(tools: list) -> list:
    """将 Responses 格式的 tools 转为 Chat 格式。

    Responses:  {"type": "function", "name": "shell", "description": ..., "parameters": ...}
    Chat:       {"type": "function", "function": {"name": "shell", "description": ..., "parameters": ...}}
    """
    result = []
    for t in tools:
        if not isinstance(t, dict):
            continue
        if t.get("type") != "function":
            continue
        # 已经是 Chat 格式（有 "function" key）
        if "function" in t:
            result.append(t)
            continue
        # Responses 扁平格式 → Chat 嵌套格式
        fn: dict[str, Any] = {"name": t.get("name", "")}
        if "description" in t:
            fn["description"] = t["description"]
        if "parameters" in t:
            fn["parameters"] = t["parameters"]
        if "strict" in t:
            fn["strict"] = t["strict"]
        result.append({"type": "function", "function": fn})
    return result


# ---------------------------------------------------------------------------
# 响应转换：Chat → Responses
# ---------------------------------------------------------------------------

class ResponsesStreamConverter:
    """将 Chat SSE 流实时转换为 Responses API 语义事件流。

    用法：
      converter = ResponsesStreamConverter(model="glm-5.2")
      # 对后端返回的每个 SSE 行调 feed_line()
      # feed_line 返回要发送给客户端的 Responses 事件字符串（可能多行）
      for line in backend_sse:
          events = converter.feed_line(line)
          if events:
              yield events.encode()
      # 流结束后调 finish() 获取收尾事件
      yield converter.finish().encode()
    """

    def __init__(self, model: str = "unknown"):
        self.resp_id = _rand_id("resp_")
        self.msg_id = _rand_id("msg_")
        self.model = model
        self.created_at = int(time.time())

        # 状态标记
        self._emitted_created = False
        self._emitted_msg_item = False

        # 累积内容
        self._content = ""
        # 思维链：上游 delta.reasoning_content。Responses 协议用 reasoning item
        # （response.reasoning_summary_text.delta）承载，此前完全没有处理，
        # 导致走 Responses 的客户端（Codex 等）永远看不到思考内容。
        self._reasoning = ""
        self._reasoning_item_id = _rand_id("rs_")
        self._emitted_reasoning_item = False
        self._reasoning_output_idx: int | None = None
        self._tool_calls: dict[int, dict] = {}  # index → {id, name, args, fc_id, output_idx, emitted}
        self._finish_reason: str | None = None
        self._usage: dict | None = None

    # ---- 公开接口 ----

    def feed_line(self, line: str) -> str:
        """处理一行 SSE（如 'data: {...}'），返回转换后的 Responses 事件字符串。"""
        line = line.strip()
        if not line or not line.startswith("data:"):
            return ""
        data = line[5:].strip()
        if data == "[DONE]":
            return ""
        try:
            chunk = json.loads(data)
        except json.JSONDecodeError:
            return ""
        return self._process_chunk(chunk)

    def finish(self) -> str:
        """流结束后，发出收尾事件（item done + completed）。

        只发 Responses 规范里的必需事件：`*_text.done` / `content_part.*` /
        `function_call_arguments.done` 属于可选扩展，严格校验的客户端会判非法
        （见 ALLOWED_RESPONSES_EVENTS 的注释）。客户端靠
        `response.output_item.done` 收尾 text/tool 块、靠 `response.completed`
        拿 finishReason，所以这三类就够。
        """
        events: list[str] = []

        # 关闭 reasoning item（先于正文，与 output 顺序一致）
        if self._emitted_reasoning_item:
            events.append(self._evt("response.reasoning_summary_part.done", {
                "item_id": self._reasoning_item_id,
                "summary_index": 0,
            }))
            # 可选扩展事件：仅老客户端需要（_evt 按开关过滤）
            events.append(self._evt("response.reasoning_summary_text.done", {
                "item_id": self._reasoning_item_id,
                "output_index": 0,
                "summary_index": 0,
                "text": self._reasoning,
            }))
            events.append(self._evt("response.output_item.done", {
                "output_index": 0,
                "item": self._reasoning_item("completed"),
            }))

        if self._emitted_msg_item:
            base = self._body_base_idx()
            events.append(self._evt("response.output_text.done", {
                "output_index": base, "content_index": 0, "text": self._content
            }))
            events.append(self._evt("response.content_part.done", {
                "output_index": base, "content_index": 0,
                "part": {"type": "output_text", "text": self._content, "annotations": []}
            }))
            events.append(self._evt("response.output_item.done", {
                "output_index": base,
                "item": self._msg_item("completed")
            }))

        # 关闭 function calls
        for idx in sorted(self._tool_calls):
            tc = self._tool_calls[idx]
            if tc.get("emitted"):
                events.append(self._evt("response.function_call_arguments.done", {
                    "item_id": tc["fc_id"],
                    "output_index": tc["output_idx"], "arguments": tc["args"]
                }))
                events.append(self._evt("response.output_item.done", {
                    "output_index": tc["output_idx"], "item": self._fc_item(tc, "completed")
                }))

        # response.completed —— 必须带 usage（客户端 schema 里 usage 是必填，
        # 且 input/output_tokens 必须是 number）。上游偶尔不给 usage 时补零，
        # 否则 completed 校验失败 → 客户端拿不到 finishReason。
        events.append(self._evt("response.completed", {
            "response": self._response_obj("completed")
        }))
        return "".join(events)

    def get_nonstream_response(self) -> dict:
        """流结束后获取完整的非流式 Response 对象。"""
        return self._response_obj("completed")

    # ---- 内部 ----

    def _body_base_idx(self) -> int:
        """正文（message item）的 output_index：reasoning item 占 0 时正文顺延到 1。"""
        return 1 if self._emitted_reasoning_item else 0

    def _reasoning_item(self, status: str) -> dict:
        """Responses 协议的 reasoning item。summary 为空时给空数组。"""
        summary = []
        if self._reasoning:
            summary = [{"type": "summary_text", "text": self._reasoning}]
        return {
            "type": "reasoning",
            "id": self._reasoning_item_id,
            "summary": summary,
            "status": status,
        }

    def _process_chunk(self, chunk: dict) -> str:
        events: list[str] = []

        # 模型名
        if chunk.get("model"):
            self.model = chunk["model"]

        # 首次 → 发 created（in_progress 不在客户端契约内，_evt 会按需过滤）
        if not self._emitted_created:
            resp = self._response_obj("in_progress")
            events.append(self._evt("response.created", {"response": resp}))
            events.append(self._evt("response.in_progress", {"response": resp}))
            self._emitted_created = True

        # usage
        if chunk.get("usage"):
            self._usage = chunk["usage"]

        for choice in chunk.get("choices", []):
            delta = choice.get("delta", {})
            finish = choice.get("finish_reason")

            # ---- reasoning delta（思维链）----
            # 映射为 Responses 的 reasoning item；output_index 固定占 0，
            # 正文 msg / function_call 依次后移（见 _body_base_idx）。
            reasoning = delta.get("reasoning_content")
            if reasoning:
                if not self._emitted_reasoning_item:
                    self._reasoning_output_idx = 0
                    events.append(self._evt("response.output_item.added", {
                        "output_index": 0,
                        "item": self._reasoning_item("in_progress"),
                    }))
                    # summary part 的开场事件：客户端靠它初始化 summary 块，
                    # 缺了会导致 reasoning 无法按 part 收尾。
                    events.append(self._evt("response.reasoning_summary_part.added", {
                        "item_id": self._reasoning_item_id,
                        "summary_index": 0,
                    }))
                    self._emitted_reasoning_item = True
                self._reasoning += reasoning
                events.append(self._evt("response.reasoning_summary_text.delta", {
                    "item_id": self._reasoning_item_id,
                    "output_index": 0,
                    "summary_index": 0,
                    "delta": reasoning,
                }))

            # ---- content delta ----
            content = delta.get("content")
            if content:
                if not self._emitted_msg_item:
                    events.append(self._evt("response.output_item.added", {
                        "output_index": self._body_base_idx(),
                        "item": self._msg_item("in_progress", empty=True)
                    }))
                    events.append(self._evt("response.content_part.added", {
                        "output_index": self._body_base_idx(), "content_index": 0,
                        "part": {"type": "output_text", "text": "", "annotations": []}
                    }))
                    self._emitted_msg_item = True

                self._content += content
                # item_id 是**必填**：规范要求 output_text.delta 为
                # {type,item_id,delta}，且 item_id 必须等于前面
                # output_item.added 里 announce 的 message id。缺了它，
                # 严格校验的客户端会把整条正文 delta 丢掉（症状：只剩思考、没有正文）。
                # output_index/content_index 是规范里的冗余字段，保留以兼容其它客户端。
                events.append(self._evt("response.output_text.delta", {
                    "item_id": self.msg_id,
                    "output_index": self._body_base_idx(), "content_index": 0,
                    "delta": content
                }))

            # ---- tool_calls delta ----
            for tc in delta.get("tool_calls", []):
                idx = tc.get("index", 0)
                if idx not in self._tool_calls:
                    # 计算 output_index：reasoning / msg 已占用前置槽位，function_call 顺延
                    base = self._body_base_idx()
                    if self._emitted_msg_item or self._content:
                        base += 1
                    oi = base + len(self._tool_calls)
                    self._tool_calls[idx] = {
                        "id": tc.get("id", ""),
                        "name": "",
                        "args": "",
                        "fc_id": _rand_id("fc_"),
                        "output_idx": oi,
                        "emitted": False,
                    }
                slot = self._tool_calls[idx]
                if tc.get("id"):
                    slot["id"] = tc["id"]
                fn = tc.get("function", {})
                if fn.get("name"):
                    slot["name"] = fn["name"]

                if not slot["emitted"]:
                    # 确保 msg item 已发出（即使 content 为空）
                    if not self._emitted_msg_item and (self._content or not self._tool_calls):
                        pass  # 不需要额外处理
                    events.append(self._evt("response.output_item.added", {
                        "output_index": slot["output_idx"],
                        "item": self._fc_item(slot, "in_progress")
                    }))
                    slot["emitted"] = True

                if fn.get("arguments"):
                    slot["args"] += fn["arguments"]
                    # item_id 同为必填（规则同 output_text.delta），需指向
                    # announce 过的 function_call item id（不是 call_id）
                    events.append(self._evt("response.function_call_arguments.delta", {
                        "item_id": slot["fc_id"],
                        "output_index": slot["output_idx"],
                        "delta": fn["arguments"]
                    }))

            if finish:
                self._finish_reason = finish

        return "".join(events)

    def _evt(self, event_type: str, data: dict) -> str:
        """格式化一个 SSE 事件；不在客户端契约内的事件直接丢弃。

        EMIT_EXTENDED_EVENTS=True 时放行 EXTENDED_ONLY_EVENTS（为兼容老客户端）。
        """
        if event_type not in ALLOWED_RESPONSES_EVENTS:
            if not (EMIT_EXTENDED_EVENTS and event_type in EXTENDED_ONLY_EVENTS):
                return ""
        payload = {"type": event_type, **data}
        return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

    def _msg_item(self, status: str = "in_progress", empty: bool = False) -> dict:
        content = [] if empty else [
            {"type": "output_text", "text": self._content, "annotations": []}
        ]
        return {
            "type": "message",
            "id": self.msg_id,
            "status": status,
            "role": "assistant",
            "content": content,
        }

    def _fc_item(self, tc: dict, status: str) -> dict:
        return {
            "type": "function_call",
            "id": tc["fc_id"],
            "call_id": tc["id"],
            "name": tc["name"],
            "arguments": tc["args"],
            "status": status,
        }

    def _response_obj(self, status: str) -> dict:
        output = []
        if self._emitted_reasoning_item or self._reasoning:
            output.append(self._reasoning_item(status))
        if self._emitted_msg_item or self._content:
            output.append(self._msg_item(status))
        for idx in sorted(self._tool_calls):
            tc = self._tool_calls[idx]
            if tc.get("emitted"):
                output.append(self._fc_item(tc, status))

        # response.completed 的对象里 usage 是必填，且 input_tokens /
        # output_tokens 必须是 number。上游偶尔不吐 usage，给 None 会导致
        # completed 不合法 → 客户端拿不到 finishReason。缺失时一律补零。
        usage = {
            "input_tokens": 0,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 0},
        }
        if self._usage:
            u = self._usage
            # reasoning_tokens 从上游 usage 真实透出（此前硬编码 0，把思维链用量抹掉了）
            details = u.get("completion_tokens_details") or {}
            reasoning_tokens = details.get("reasoning_tokens")
            if reasoning_tokens is None:
                reasoning_tokens = u.get("completion_thinking_tokens", 0)
            prompt_details = (u.get("prompt_tokens_details") or {}).get("cached_tokens", 0)
            usage = {
                "input_tokens": u.get("prompt_tokens", u.get("input_tokens", 0)) or 0,
                "input_tokens_details": {
                    "cached_tokens": prompt_details or u.get("cached_tokens", 0) or 0
                },
                "output_tokens": u.get("completion_tokens", u.get("output_tokens", 0)) or 0,
                "output_tokens_details": {"reasoning_tokens": reasoning_tokens or 0},
                "total_tokens": u.get("total_tokens", 0) or 0,
            }

        return {
            "id": self.resp_id,
            "object": "response",
            "created_at": self.created_at,
            "status": status,
            "model": self.model,
            "output": output,
            "parallel_tool_calls": True,
            "usage": usage,
        }
