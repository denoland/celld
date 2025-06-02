# CellVerse Testing Guide - Phase 5

## Multi-User Testing

### Setup
1. Start the cells backend (if not already running):
   ```bash
   cd demos/cellverse
   docker run -e RUST_LOG=info -e CELL_DENO_OUTPUT=1 -e CELL_GRACE_PERIOD_SECONDS=1 -e OPENAI_API_KEY=$OPENAI_API_KEY -p 8000:8000 -v $PWD:/app cells:latest cells/main.ts
   ```

2. Start the frontend dev server:
   ```bash
   npm run dev:fe
   ```

### Test Steps

#### 1. Multi-User Chat Test
1. Open CellVerse in two different browser windows:
   - Window 1: Regular browser window at http://localhost:5173
   - Window 2: Incognito/Private window at http://localhost:5173

2. Log in as two different GitHub users (or same user in different sessions)

3. In both windows:
   - Create or join the same channel (e.g., "test-chat")
   
4. Test real-time messaging:
   - Send a message from Window 1
   - Verify it appears instantly in Window 2
   - Send a message from Window 2
   - Verify it appears instantly in Window 1
   - Verify AI assistant responds to both users

5. Expected results:
   - Messages appear in real-time for all users
   - Each user sees their own messages styled differently (right-aligned)
   - Usernames are displayed correctly
   - AI responses appear for all users

#### 2. Persistence Test
1. Have a conversation in a channel with multiple messages

2. Note the channel name and messages

3. Close all browser windows

4. Stop the Docker container (Ctrl+C)

5. Restart the Docker container with the same command

6. Open CellVerse and log in again

7. Join the same channel

8. Expected results:
   - All previous messages are loaded
   - Message order is preserved
   - Usernames and timestamps are correct
   - AI responses are marked correctly

## Test Checklist

- [ ] Multi-user real-time messaging works
- [ ] Messages from different users are visually distinct
- [ ] Usernames display correctly
- [ ] AI assistant responds to all users
- [ ] Messages persist after container restart
- [ ] Channel list persists
- [ ] User authentication persists (JWT in localStorage)
- [ ] No duplicate messages appear
- [ ] Connection status updates correctly
- [ ] Error handling works (try disconnecting network)

## Known Issues to Test

1. **Duplicate Channels**: Try creating a channel with the same name
   - Should return "Channel already exists" error

2. **Long Messages**: Send very long messages
   - Should wrap properly and not break layout

3. **Special Characters**: Try channel names with special characters
   - Should create valid slugs

4. **Concurrent Messages**: Send messages simultaneously from both windows
   - All messages should appear in correct order

## Performance Testing

1. Send many messages rapidly
2. Join channel with 50+ message history
3. Create 10+ channels
4. Keep connection open for extended period

## Edge Cases

1. Login with GitHub, then revoke app access on GitHub
2. Join non-existent channel directly via URL manipulation
3. Send message while disconnected
4. Create channel with empty name
5. Send empty messages