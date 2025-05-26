from openai.types.chat import (
    ParsedChatCompletion,
    ParsedChatCompletionMessage,
    ParsedChoice,
)

DEFAULT_OPENAI_KWARGS = {
    "model": "gpt-4.1-mini-2025-04-14",
    "temperature": 0.0,
    "timeout": 30.0,
}

DEFAULT_DATABRICKS_KWARGS = {
    "model": "databricks-meta-llama-3-3-70b-instruct",
    "temperature": 0.0,
    "timeout": 30.0,
    "max_tokens": 256,
}

DEFAULT_EMPTY_PARSED_COMPLETION = ParsedChatCompletion(
    id="",
    choices=[ParsedChoice(finish_reason="stop", index=0, message=ParsedChatCompletionMessage(role="assistant"))],
    created=0,
    model="",
    object="chat.completion"
)
