"""
连续追问监听器插件
允许用户在 Bot 回复后的一段时间内，通过特定符号或关键词触发连续追问，
无需每次都 @ Bot。
"""
import asyncio
import json
from asyncio import Lock

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register


@register(
    "astrbot_plugin_followup_listener",
    "kelinton",
    "连续追问监听器 - 在群聊中开启限时监听窗口，用户发送包含特定符号或关键词的消息时自动触发连续回复",
    "1.0.3",
    "https://github.com/kelinton/astrbot_plugin_followup_listener",
)
class FollowUpListenerPlugin(Star):
    """
    连续追问监听器插件

    业务逻辑:
    1. 当 Bot 在群聊中完成一次正常回复后，记录该用户的 ID，并开启一个可计时的异步监听窗口
    2. 在监听时间内，如果该用户发送了新的消息，且消息中包含特定符号或关键词，
       则自动跳过"必须 @ Bot"的限制，直接调用 LLM 进行连续回复
    3. 如果消息不包含触发条件，或者超过了监听时间，则忽略该消息，按正常流程处理
    """

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)

        # 绑定插件配置
        self.config = config

        # 初始化存储监听状态的字典
        # key: umo (unified_msg_origin)
        # value: {"user_id": str, "expire_time": float, "history": list, "timer": TimerHandle}
        self.listeners = {}

        # 异步锁，确保对 self.listeners 的访问是线程安全的
        self.lock = Lock()

        logger.info("[FollowUpListener] 插件已加载！")
        logger.debug(f"[FollowUpListener] 插件配置：{self.config}")

    async def _arm_listener(self, umo: str, user_id: str, history: list, duration: int):
        """
        激活针对某个会话的监听器

        Args:
            umo: 统一消息来源 (unified_msg_origin)
            user_id: 用户 ID
            history: 当前对话历史记录
            duration: 监听时长（秒）
        """
        # 先取消旧的定时器（如果存在）
        async with self.lock:
            if umo in self.listeners:
                old_timer = self.listeners[umo].get("timer")
                if old_timer:
                    old_timer.cancel()

            # 计算过期时间
            loop = asyncio.get_running_loop()
            expire_time = loop.time() + duration

            # 设置定时器，到期自动清理
            timer = loop.call_later(duration, self._clear_listener_sync, umo)

            self.listeners[umo] = {
                "user_id": user_id,
                "expire_time": expire_time,
                "history": history,
                "timer": timer
            }

        logger.debug(f"[FollowUpListener] 已为会话 {umo[:30]}... 开启监听，时长 {duration}s")

    def _clear_listener_sync(self, umo: str):
        """
        同步版本的清理回调函数，用于 call_later

        由于 call_later 不能调度协程，需要一个同步的包装器
        """
        # 创建一个新任务来执行异步清理
        try:
            loop = asyncio.get_running_loop()
            asyncio.ensure_future(self._clear_listener(umo), loop=loop)
        except RuntimeError:
            # 如果事件循环不可用，直接同步清理
            if umo in self.listeners:
                self.listeners.pop(umo, None)

    async def _clear_listener(self, umo: str):
        """
        清除过期的监听器
        """
        async with self.lock:
            if umo in self.listeners:
                self.listeners.pop(umo, None)
                logger.debug(f"[FollowUpListener] 会话 {umo[:30]}... 监听已过期并清理")

    def _check_trigger(self, message: str) -> bool:
        """
        检查消息是否包含配置的触发符号或关键词

        Args:
            message: 用户发送的消息文本

        Returns:
            bool: 是否触发
        """
        symbols = self.config.get("trigger_symbols", [])
        keywords = self.config.get("trigger_keywords", [])

        # 检查触发符号
        for sym in symbols:
            if sym and sym in message:
                return True

        # 检查触发关键词
        for kw in keywords:
            if kw and kw in message:
                return True

        return False

    async def _get_conversation_history(self, umo: str) -> list:
        """
        从对话管理器获取当前对话历史

        Args:
            umo: 统一消息来源

        Returns:
            list: 对话历史记录，格式为 [{"role": "user/assistant", "content": "..."}, ...]
        """
        try:
            curr_cid = await self.context.conversation_manager.get_curr_conversation_id(umo)
            if not curr_cid:
                return []

            conversation = await self.context.conversation_manager.get_conversation(umo, curr_cid)
            if not conversation:
                return []
            if conversation.history:
                return json.loads(conversation.history)
        except Exception as e:
            logger.error(f"[FollowUpListener] 获取对话历史时出错：{e}", exc_info=True)

        return []

    @filter.after_message_sent()
    async def after_bot_message_sent(self, event: AstrMessageEvent):
        """
        钩子：当 Bot 发送完消息后，激活对该用户所在会话的监听

        这是连续追问的触发前提：只有 Bot 刚回复过，才会开启监听窗口
        """
        # 检查插件总开关
        if not self.config.get("enable", True):
            return
        # 检查私聊并跳过
        if event.is_private_chat():
            return

        # 获取群 ID 和用户 ID
        group_id = event.get_group_id()
        user_id = event.get_sender_id()

        # 如果没有群 ID 或用户 ID，或者消息是机器人自己发的，直接返回
        if not group_id or not user_id:
            return
        if user_id == event.get_self_id():
            return

        umo = event.unified_msg_origin

        # 获取配置的监听时长
        listen_duration = self.config.get("listen_duration", 30)

        # 获取当前的对话历史
        history = await self._get_conversation_history(umo)

        # 武装监听器
        await self._arm_listener(umo, user_id, history, listen_duration)

        logger.info(f"[FollowUpListener] 已为群 {group_id} 的用户 {user_id} 开启 {listen_duration}s 的监听窗口")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        """
        钩子：处理群聊消息，检测是否符合连续追问条件

        如果在监听窗口内收到符合条件的消息，直接调用 LLM 回复，无需 @ Bot
        """
        # 检查插件总开关
        if not self.config.get("enable", True):
            return

        # 获取事件信息
        group_id = event.get_group_id()
        sender_id = event.get_sender_id()

        # 如果不是群聊消息，或者消息是机器人自己发的，直接返回
        if not group_id or not sender_id:
            return
        if sender_id == event.get_self_id():
            return

        umo = event.unified_msg_origin
        message_str = event.message_str.strip()

        # 如果消息为空，不处理
        if not message_str:
            return

        # 检查是否有活跃的监听器
        async with self.lock:
            listener_data = self.listeners.get(umo)

        # 如果没有活跃的监听器，或者不是刚才被监听的同一位用户，直接放行
        if not listener_data:
            return
        if listener_data["user_id"] != sender_id:
            return

        # 检查是否触发了特定的符号或关键词
        if not self._check_trigger(message_str):
            logger.debug(f"[FollowUpListener] 消息未命中触发条件：{message_str[:50]}...")
            return
        event.stop_event()  # 阻止事件继续传播，防止触发正常的命令匹配

        logger.info(f"[FollowUpListener] 检测到连续追问：用户 {sender_id} - 消息：{message_str[:50]}...")

        try:
            # 获取 LLM 提供商
            provider = self.context.get_using_provider()
            if not provider:
                logger.warning("[FollowUpListener] 未找到可用的 LLM 提供商")
                yield event.plain_result()
                return

            # 使用之前捕获的历史记录作为上下文
            history = listener_data.get("history", [])

            # 构造系统提示
            system_prompt = self.config.get("system_prompt", "")
            if not system_prompt:
                system_prompt = ""

            # 调用 LLM 生成回复
            llm_response = await provider.text_chat(
                prompt=message_str,
                contexts=history,
                system_prompt=system_prompt
            )

            reply_text = llm_response.completion_text

            # 发送回复
            yield event.plain_result(reply_text)
            logger.info(f"[FollowUpListener] 追问回复已发送：{reply_text[:50]}...")

            # 更新对话管理器的历史记录，保持上下文连贯
            cid = await self.context.conversation_manager.get_curr_conversation_id(umo)
            if cid:
                # 构造更新后的历史记录
                updated_history = history + [
                    {"role": "user", "content": message_str},
                    {"role": "assistant", "content": reply_text}
                ]
                await self.context.conversation_manager.update_conversation(umo, cid, updated_history)
                logger.debug(f"[FollowUpListener] 已更新对话历史：{cid[:8]}...")

        except Exception as e:
            logger.error(f"[FollowUpListener] 追问处理出错：{e}", exc_info=True)
            yield event.plain_result()

        finally:
            # 清除监听器
            await self._clear_listener(umo)
            logger.debug(f"[FollowUpListener] 已清除会话 {umo[:30]}... 的监听器")

    async def terminate(self):
        """
        插件卸载时的资源清理
        """
        logger.info("[FollowUpListener] 正在卸载插件")

        async with self.lock:
            # 取消所有定时器，不知道有没有用，还是加上吧
            for umo, data in self.listeners.items():
                timer = data.get("timer")
                if timer:
                    timer.cancel()
            self.listeners.clear()

        logger.info("[FollowUpListener] 插件已卸载")
