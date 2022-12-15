from typing import List, Optional, Tuple
from dataclasses import dataclass

@dataclass
class Provernance:
    activity: str
    actor: dict # id,type,name,homepage
    generated: Optional[dict] # id (doc_id), 
    used: Optional[dict] #id, url (target id, target url)
    output_location: Optional[str] # output_dir
    processingTime: Optional[int]
    downloadTime: Optional[int]
    extra_info: Optional[dict]
    softwareversion: dict # kaldi_nl version, kaldi_nl_git url