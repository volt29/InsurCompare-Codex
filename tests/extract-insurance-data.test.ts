import assert from 'node:assert/strict';
import test from 'node:test';
import { spawnSync } from 'node:child_process';

import {
  buildSegmentsFromText,
  extractPlainTextFromMinerUArchive,
  persistExtractedPlainText,
  processMinerUArchiveAndPersist,
  sendSegmentsToLovable,
} from '../supabase/functions/extract-insurance-data/index.ts';

test('extractPlainTextFromMinerUArchive merges markdown.md and text.txt into a single string', async () => {
  const archive = createZipArchive({
    'markdown.md': '# Header',
    'nested/text.txt': 'Additional text',
  });

  const text = await extractPlainTextFromMinerUArchive(archive);
  assert.strictEqual(text, '# Header\n\nAdditional text');
});

test('extractPlainTextFromMinerUArchive throws when archive is missing supported files', async () => {
  const archive = createZipArchive({ 'other.txt': 'Nothing useful here' });
  await assert.rejects(() => extractPlainTextFromMinerUArchive(archive), /did not contain markdown\.md nor text\.txt/i);
});

test('buildSegmentsFromText splits plain string into evenly sized segments', () => {
  const segments = buildSegmentsFromText('abcdefghijkl', 5);
  assert.deepStrictEqual(segments, ['abcde', 'fghij', 'kl']);
});

test('buildSegmentsFromText rejects non-string input', () => {
  assert.throws(() => buildSegmentsFromText(123, 5), /plain string/);
});

test('sendSegmentsToLovable rejects empty text payloads', async () => {
  const sender = async () => undefined;
  await assert.rejects(() => sendSegmentsToLovable('', sender), /cannot send empty text/i);
});

test('sendSegmentsToLovable sends segments sequentially and logs payload sizes', async () => {
  const calls = [];
  const originalLog = console.log;
  console.log = (...args) => {
    calls.push(args.join(' '));
  };

  const payloads = [];
  const sender = async (payload) => {
    payloads.push(payload);
  };

  try {
    await sendSegmentsToLovable('abcdefghij', sender, 4);
  } finally {
    console.log = originalLog;
  }

  assert.deepStrictEqual(payloads, [
    { content: 'abcd', index: 1, total: 3 },
    { content: 'efgh', index: 2, total: 3 },
    { content: 'ij', index: 3, total: 3 },
  ]);

  assert.ok(calls.some((line) => /received 10 characters/.test(line)));
  assert.ok(calls.includes('Segment 1/3 wysłany do AI'));
});

test('persistExtractedPlainText persists plain text and logs character count', async () => {
  const logs = [];
  const updatePayloads = [];
  const originalLog = console.log;
  console.log = (...args) => {
    logs.push(args.join(' '));
  };

  const supabaseClient = {
    from: () => ({
      update: (values) => {
        updatePayloads.push(values);
        return {
          eq: (column, value) => ({
            select: async () => ({ data: [{ id: value }], error: null }),
          }),
        };
      },
    }),
  };

  try {
    const result = await persistExtractedPlainText({
      supabaseClient,
      table: 'documents',
      documentId: 'doc-1',
      plainText: 'Plain text body',
    });

    assert.strictEqual(result.charCount, 15);
    assert.deepStrictEqual(updatePayloads[0], {
      extracted_text: 'Plain text body',
      extracted_text_char_count: 15,
    });
    assert.ok(logs.some((line) => /15 characters/.test(line)));
  } finally {
    console.log = originalLog;
  }
});

test('persistExtractedPlainText throws when Supabase reports an error', async () => {
  const supabaseClient = {
    from: () => ({
      update: () => ({
        eq: () => ({
          select: async () => ({ data: null, error: { message: 'boom' } }),
        }),
      }),
    }),
  };

  await assert.rejects(
    () =>
      persistExtractedPlainText({
        supabaseClient,
        table: 'documents',
        documentId: 'doc-1',
        plainText: 'anything',
      }),
    /boom/,
  );
});

test('persistExtractedPlainText throws when no rows are updated', async () => {
  const supabaseClient = {
    from: () => ({
      update: () => ({
        eq: () => ({
          select: async () => ({ data: [], error: null }),
        }),
      }),
    }),
  };

  await assert.rejects(
    () =>
      persistExtractedPlainText({
        supabaseClient,
        table: 'documents',
        documentId: 'doc-1',
        plainText: 'anything',
      }),
    /did not modify any document/i,
  );
});

test('processMinerUArchiveAndPersist processes archive end-to-end and reports character count', async () => {
  const archive = createZipArchive({
    'markdown.md': 'Line one',
    'text.txt': 'Line two',
  });

  const logs = [];
  const originalLog = console.log;
  console.log = (...args) => {
    logs.push(args.join(' '));
  };

  const updatePayloads = [];

  const supabaseClient = {
    from: () => ({
      update: (values) => {
        updatePayloads.push(values);
        return {
          eq: () => ({
            select: async () => ({ data: [{ id: 'doc-1' }], error: null }),
          }),
        };
      },
    }),
  };

  const sentSegments = [];
  const sender = async (payload) => {
    sentSegments.push(payload);
  };

  try {
    const result = await processMinerUArchiveAndPersist({
      archiveBuffer: archive,
      supabaseClient,
      tableName: 'documents',
      documentId: 'doc-1',
      sender,
    });

    assert.strictEqual(result.plainText, 'Line one\n\nLine two');
    assert.strictEqual(result.charCount, 18);
    assert.strictEqual(result.segmentsSent, 1);
    assert.deepStrictEqual(sentSegments, [
      { content: 'Line one\n\nLine two', index: 1, total: 1 },
    ]);

    assert.ok(logs.some((line) => /received 18 characters/.test(line)));
    assert.ok(logs.some((line) => /Segment 1\/1 wysłany do AI/.test(line)));
    assert.strictEqual(updatePayloads[0].extracted_text, 'Line one\n\nLine two');
    assert.strictEqual(updatePayloads[0].extracted_text_char_count, 18);
  } finally {
    console.log = originalLog;
  }
});

function createZipArchive(files) {
  const script = `
import io
import json
import sys
import zipfile

files = json.loads(sys.stdin.read())
buffer = io.BytesIO()
with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
    for name, content in files.items():
        archive.writestr(name, content)

sys.stdout.buffer.write(buffer.getvalue())
`;

  const result = spawnSync('python3', ['-c', script], {
    input: JSON.stringify(files),
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    throw new Error(result.stderr.toString() || 'Failed to generate archive');
  }

  return result.stdout ?? Buffer.alloc(0);
}
