"""Initial Testing First Model for Fitz DD"""

design_name = 'DD-Model1'
contrasts = [
  ('all trials', ['immediate', 'delayed'], [1, 1]),                # 1
  ('choice',     ['immediatexchoiceInt^1', 'delayedxchoiceInt^1'], [1, 1])  # 2
]
