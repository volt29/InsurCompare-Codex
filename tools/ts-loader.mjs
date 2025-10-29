import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

export async function resolve(specifier, context, defaultResolve) {
  if (specifier.endsWith('.ts')) {
    const parentURL = context.parentURL ?? pathToFileURL(`${process.cwd()}/`);
    const url = new URL(specifier, parentURL);
    return { url: url.href, shortCircuit: true };
  }

  return defaultResolve(specifier, context, defaultResolve);
}

export async function load(url, context, defaultLoad) {
  if (url.endsWith('.ts')) {
    const source = await readFile(new URL(url));
    return { format: 'module', source: source.toString(), shortCircuit: true };
  }

  return defaultLoad(url, context, defaultLoad);
}
