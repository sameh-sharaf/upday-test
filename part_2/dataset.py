import uuid
import random
import time
import datetime

# Set initial ID
INIT_ID = 10e2

# Set no. of user IDs
USER_IDS = 5

# Set max no. of session IDs per user
SESSION_IDS = 10

# Max records per session
MAX_RECORDS_PER_SESSION = 25

# Set event max duration in seconds
MAX_DURATION = 600

# Initial timestamp
INIT_TIMESTAMP = datetime.datetime(2018, 3, 1, 17, 5, 52)

# Initial timestamp delta in seconds (here 10 days)
INIT_DELTA_SECONDS = 3600 * 24 * 10

# List of events
events = ['signed_up', 'article_read', 'article_shared', 'article_liked']

# Session ID iterator
iter = 0

# List of lines
lines = []

for i in range(0, USER_IDS):
  # Generate user ID
  userId = 'user-%s' % uuid.uuid4().hex[:4]

  # Generate sessions for each user
  for j in range(0, random.randint(1, SESSION_IDS)):
    sessionId = 'session-%s' % uuid.uuid4().hex[:6]
    signedUp = False
    sessionTimestamp = INIT_TIMESTAMP + datetime.timedelta(seconds=random.randint(0, INIT_DELTA_SECONDS))

    for k in range(0, random.randint(1, MAX_RECORDS_PER_SESSION)):
      eventName = events[random.randint(0, len(events) - 1)]
      if eventName == 'signed_up':
        if signedUp:
          continue
        signedUp = True
      
      eventDuration = random.randint(0, MAX_DURATION)
      at = sessionTimestamp

      # Add duration of current event to session timestamp
      sessionTimestamp +=  datetime.timedelta(seconds=eventDuration)
    
      # Iterate ID
      iter += 1

      # Append new record
      lines.append('%d,%s,%s,%s,%d,%s' % (INIT_ID + iter, userId, sessionId, eventName, eventDuration, at.isoformat()))

# Write records to CSV file    
filename = 'output-%d.csv' % (time.time())
with open(filename, 'w') as out:
  for line in lines:
    out.write(line + '\n')