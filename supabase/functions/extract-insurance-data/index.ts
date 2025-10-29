import { inflateRawSync } from 'node:zlib';

const PRIORITISED_ARCHIVE_FILES = ['markdown.md', 'text.txt'];

const textDecoder = new TextDecoder('utf-8');

/**
 * @typedef {{
 *   content: string;
 *   index: number;
 *   total: number;
 * }} SegmentSenderPayload
 */

/**
 * @typedef {{
 *   data: Array<{ id: string | number }> | null;
 *   error: { message?: string } | null;
 * }} SupabaseSelectResponse
 */

/**
 * @typedef {{
 *   supabaseClient: SupabaseClientLike;
 *   table: string;
 *   documentId: string | number;
 *   plainText: string;
 *   additionalFields?: Record<string, unknown>;
 * }} PersistOptions
 */

/**
 * @typedef {{
 *   archiveBuffer: ArrayBuffer | Uint8Array | Buffer;
 *   supabaseClient: SupabaseClientLike;
 *   tableName: string;
 *   documentId: string | number;
 *   sender: (payload: SegmentSenderPayload) => Promise<void> | void;
 *   segmentLength?: number;
 *   additionalFields?: Record<string, unknown>;
 * }} ProcessOptions
 */

/**
 * @typedef {{
 *   from(table: string): {
 *     update(values: Record<string, unknown>): {
 *       eq(column: string, value: unknown): {
 *         select(columns?: string): Promise<SupabaseSelectResponse>;
 *       };
 *     };
 *   };
 * }} SupabaseClientLike
 */

/**
 * Extracts the concatenated plain text from a MinerU archive.
 * The archive must contain at least one of markdown.md or text.txt.
 *
 * @param {ArrayBuffer | Uint8Array | Buffer} archive
 * @returns {Promise<string>}
 */
export async function extractPlainTextFromMinerUArchive(archive) {
  const archiveView = bufferToUint8Array(archive);
  const entries = readZipEntries(archiveView);

  const collected = [];
  let order = 0;

  for (const entry of entries) {
    if (entry.isDirectory) {
      continue;
    }

    const filename = entry.name.split('/').pop();
    if (!filename) {
      continue;
    }

    const priority = PRIORITISED_ARCHIVE_FILES.findIndex((candidate) => candidate === filename.toLowerCase());
    if (priority === -1) {
      continue;
    }

    const fileContent = decodeEntryContent(entry, archiveView).trim();
    collected.push({ priority, order: order += 1, content: fileContent });
  }

  if (collected.length === 0) {
    throw new Error('MinerU archive did not contain markdown.md nor text.txt.');
  }

  collected.sort((a, b) => {
    if (a.priority !== b.priority) {
      return a.priority - b.priority;
    }

    return a.order - b.order;
  });

  const combined = collected
    .map((item) => item.content)
    .filter((content) => content.length > 0)
    .join('\n\n');

  if (!combined || combined.trim().length === 0) {
    throw new Error('MinerU archive plain text is empty.');
  }

  return combined;
}

/**
 * Splits the plain text into segments of the provided length.
 *
 * @param {string} text
 * @param {number} [maxSegmentLength=8000]
 * @returns {string[]}
 */
export function buildSegmentsFromText(text, maxSegmentLength = 8000) {
  if (typeof text !== 'string') {
    throw new TypeError('Segment generator expects a plain string input.');
  }

  if (!Number.isFinite(maxSegmentLength) || maxSegmentLength <= 0) {
    throw new RangeError('maxSegmentLength must be a positive number.');
  }

  const normalised = text.replace(/\r\n/g, '\n');
  const segments = [];

  for (let index = 0; index < normalised.length; index += maxSegmentLength) {
    segments.push(normalised.slice(index, index + maxSegmentLength));
  }

  return segments;
}

/**
 * Sends the generated segments to Lovable.
 *
 * @param {string} text
 * @param {(payload: SegmentSenderPayload) => Promise<void> | void} sender
 * @param {number} [maxSegmentLength=8000]
 * @returns {Promise<number>}
 */
export async function sendSegmentsToLovable(text, sender, maxSegmentLength = 8000) {
  if (typeof text !== 'string') {
    throw new TypeError('Lovable integration expects plain string text.');
  }

  const plainText = text;
  const charCount = plainText.length;

  console.log(`ℹ️ Lovable integration received ${charCount} characters of plain text.`);

  if (charCount === 0) {
    throw new Error('Lovable integration cannot send empty text.');
  }

  const segments = buildSegmentsFromText(plainText, maxSegmentLength);

  for (let index = 0; index < segments.length; index += 1) {
    const payload = {
      content: segments[index],
      index: index + 1,
      total: segments.length,
    };

    await sender(payload);
    console.log(`Segment ${payload.index}/${payload.total} wysłany do AI`);
  }

  return segments.length;
}

/**
 * Persists the plain text extracted from MinerU in Supabase.
 *
 * @param {PersistOptions} options
 * @returns {Promise<{ data: Array<{ id: string | number }>; charCount: number }>}
 */
export async function persistExtractedPlainText(options) {
  const { supabaseClient, table, documentId, plainText, additionalFields = {} } = options;

  console.log('✅ Step 22: Updating document with extracted data...');

  const charCount = plainText.length;
  const updatePayload = {
    extracted_text: plainText,
    extracted_text_char_count: charCount,
    ...additionalFields,
  };

  const { data, error } = await supabaseClient
    .from(table)
    .update(updatePayload)
    .eq('id', documentId)
    .select('id');

  if (error) {
    console.error('❌ Failed to update document with extracted text.', error);
    throw new Error(error.message ?? 'Unknown error when updating Supabase.');
  }

  if (!data || data.length === 0) {
    throw new Error('Supabase update did not modify any document.');
  }

  console.log(`✅ Document ${String(data[0].id ?? documentId)} updated with ${charCount} characters of extracted text.`);

  return { data, charCount };
}

/**
 * Processes the MinerU archive, sends segments to Lovable and persists the plain text.
 *
 * @param {ProcessOptions} options
 * @returns {Promise<{ plainText: string; charCount: number; segmentsSent: number }>}
 */
export async function processMinerUArchiveAndPersist(options) {
  const plainText = await extractPlainTextFromMinerUArchive(options.archiveBuffer);
  const segmentsSent = await sendSegmentsToLovable(plainText, options.sender, options.segmentLength);
  const { charCount } = await persistExtractedPlainText({
    supabaseClient: options.supabaseClient,
    table: options.tableName,
    documentId: options.documentId,
    plainText,
    additionalFields: options.additionalFields,
  });

  return { plainText, charCount, segmentsSent };
}

/**
 * @param {ArrayBuffer | Uint8Array | Buffer} input
 * @returns {Uint8Array}
 */
function bufferToUint8Array(input) {
  if (input instanceof Uint8Array) {
    return input;
  }

  if (input instanceof ArrayBuffer) {
    return new Uint8Array(input);
  }

  if (ArrayBuffer.isView(input)) {
    return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  }

  return new Uint8Array(input);
}

/**
 * @param {ZipEntry} entry
 * @param {Uint8Array} archive
 * @returns {string}
 */
function decodeEntryContent(entry, archive) {
  const start = entry.dataOffset;
  const end = start + entry.compressedSize;

  if (end > archive.length) {
    throw new Error(`Archive entry ${entry.name} exceeds archive bounds.`);
  }

  const slice = archive.subarray(start, end);

  switch (entry.compressionMethod) {
    case 0:
      return textDecoder.decode(slice);
    case 8:
      return textDecoder.decode(inflateRawSync(slice));
    default:
      throw new Error(`Unsupported compression method: ${entry.compressionMethod}`);
  }
}

/**
 * @typedef {{
 *   name: string;
 *   compressedSize: number;
 *   uncompressedSize: number;
 *   compressionMethod: number;
 *   dataOffset: number;
 *   isDirectory: boolean;
 * }} ZipEntry
 */

/**
 * @param {Uint8Array} archive
 * @returns {ZipEntry[]}
 */
function readZipEntries(archive) {
  const eocdOffset = findEndOfCentralDirectory(archive);

  const totalEntries = readUInt16LE(archive, eocdOffset + 10);
  const centralDirectoryOffset = readUInt32LE(archive, eocdOffset + 16);

  const entries = [];
  let pointer = centralDirectoryOffset;

  for (let index = 0; index < totalEntries; index += 1) {
    const signature = readUInt32LE(archive, pointer);
    if (signature !== 0x02014b50) {
      throw new Error('Invalid central directory signature.');
    }

    const generalPurpose = readUInt16LE(archive, pointer + 8);
    const compressionMethod = readUInt16LE(archive, pointer + 10);
    const compressedSize = readUInt32LE(archive, pointer + 20);
    const uncompressedSize = readUInt32LE(archive, pointer + 24);
    const fileNameLength = readUInt16LE(archive, pointer + 28);
    const extraFieldLength = readUInt16LE(archive, pointer + 30);
    const commentLength = readUInt16LE(archive, pointer + 32);
    const localHeaderOffset = readUInt32LE(archive, pointer + 42);

    const nameStart = pointer + 46;
    const nameEnd = nameStart + fileNameLength;
    const name = textDecoder.decode(archive.subarray(nameStart, nameEnd));

    const isDirectory = name.endsWith('/');
    const dataOffset = calculateDataOffset(archive, localHeaderOffset);

    if (generalPurpose & 0x08) {
      throw new Error('Streaming ZIP entries are not supported.');
    }

    entries.push({
      name,
      compressedSize,
      uncompressedSize,
      compressionMethod,
      dataOffset,
      isDirectory,
    });

    pointer = nameEnd + extraFieldLength + commentLength;
  }

  return entries;
}

/**
 * @param {Uint8Array} archive
 * @param {number} localHeaderOffset
 * @returns {number}
 */
function calculateDataOffset(archive, localHeaderOffset) {
  const signature = readUInt32LE(archive, localHeaderOffset);
  if (signature !== 0x04034b50) {
    throw new Error('Invalid local file header signature.');
  }

  const fileNameLength = readUInt16LE(archive, localHeaderOffset + 26);
  const extraFieldLength = readUInt16LE(archive, localHeaderOffset + 28);

  return localHeaderOffset + 30 + fileNameLength + extraFieldLength;
}

/**
 * @param {Uint8Array} archive
 * @returns {number}
 */
function findEndOfCentralDirectory(archive) {
  for (let index = archive.length - 22; index >= 0; index -= 1) {
    if (readUInt32LE(archive, index) === 0x06054b50) {
      return index;
    }
  }

  throw new Error('End of central directory not found.');
}

/**
 * @param {Uint8Array} archive
 * @param {number} offset
 * @returns {number}
 */
function readUInt16LE(archive, offset) {
  return archive[offset] | (archive[offset + 1] << 8);
}

/**
 * @param {Uint8Array} archive
 * @param {number} offset
 * @returns {number}
 */
function readUInt32LE(archive, offset) {
  return (
    archive[offset] |
    (archive[offset + 1] << 8) |
    (archive[offset + 2] << 16) |
    (archive[offset + 3] << 24)
  ) >>> 0;
}
