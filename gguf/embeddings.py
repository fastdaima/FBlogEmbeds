from core.models import EmbeddingModel
from core.utils import weak_lru
from llama_cpp import Llama

class GgufEmbeddings(EmbeddingModel):
    def __init__(self, model_path, model_id=None):
        self.model_id = model_id
        self.model_path = model_path 
        self._model = self._get_model()

    @weak_lru(maxsize=128)
    def _get_model(self):
        return Llama(
                model_path=self.model_path, 
                embedding=True,
                verbose=False)

    def embed_batch(self, texts):
        results = self._model.create_embedding(texts)
        return [result['embedding'] for result in results['data']]





# /home/srk/Desktop/projects/FBlogEmbeds/models/mxbai-embed-xsmall-v1-q8_0.gguf