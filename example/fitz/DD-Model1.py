"""Initial Testing First Model for Fitz DD"""

design_name = 'DD-Combined'
# conditions = ['immediate', 'delayed']
condition_col = 'immediacy'
onset_col = 'cuesTime'
duration_col = 'trialResp.rt'
pmod_cols = ['choiceInt']

contrasts = [
  ('all trials', ['immediate', 'delayed'], [1, 1]),                # 1
  ('choice',     ['immediatexchoiceInt^1', 'delayedxchoiceInt^1'], [1, 1])  # 2
]
