import pickle
from dataflow import TokenBucketState

state = TokenBucketState()
state.tokens = 3.0
state.last_update = 100.0

pickled = pickle.dumps(state)
unpickled = pickle.loads(pickled)
print(unpickled.tokens, unpickled.last_update)
