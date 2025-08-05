from typing import Optional
from pydantic import BaseModel, model_validator, Field

class SynthesisRequest(BaseModel):
    apikey: str = Field(..., description="用于身份验证的API密钥。")
    target: str = Field(..., description="需要被合成为音频的目标文本。")
    voice: Optional[str] = Field(None, description="要使用的预设音色名称。")
    voice_url: Optional[str] = Field(None, description="一个指向外部音频文件的URL，用作即时克隆的音色样本。")
    voice_prompt_text: Optional[str] = Field(None, description="voice_url所提供音频文件对应的文本内容。当提供voice_url时，此项为必需。")

    @model_validator(mode='after')
    def check_voice_source(self) -> 'SynthesisRequest':
        if self.voice is None and self.voice_url is None:
            raise ValueError('必须提供 "voice" 或 "voice_url" 两者之一。')
        if self.voice is not None and self.voice_url is not None:
            raise ValueError('只能提供 "voice" 或 "voice_url" 两者之一，不能同时提供。')
        if self.voice_url and not self.voice_prompt_text:
            raise ValueError('当提供 "voice_url" 时，必须同时提供 "voice_prompt_text"。')
        if self.voice and self.voice_prompt_text:
            raise ValueError('当使用预设音色 "voice" 时，不应提供 "voice_prompt_text"。')
        return self