import type {
  Completion,
  CompletionContext,
  CompletionResult,
  CompletionSource,
} from '@codemirror/autocomplete';

import type { ObservedSchema } from './schema';
import { escapeGqlIdentifier } from './schema';

export type GqlCompletionKind = 'label' | 'relationshipType' | 'keyword';

export interface GqlCompletionQuery {
  readonly kind: GqlCompletionKind;
  /** Document offset where the partial word being completed starts. */
  readonly from: number;
}

const KEYWORDS = [
  'AND',
  'AS',
  'ASC',
  'BY',
  'DESC',
  'DISTINCT',
  'EXISTS',
  'FALSE',
  'LIMIT',
  'MATCH',
  'NOT',
  'NULL',
  'OFFSET',
  'OPTIONAL',
  'OR',
  'ORDER',
  'RETURN',
  'SKIP',
  'TRUE',
  'UNION',
  'UNWIND',
  'WHERE',
  'WITH',
] as const;

const WORD_TAIL = /[A-Za-z_$][A-Za-z0-9_$]*$/;
const SAFE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Decides what to complete at `pos`: labels directly after a `:` inside a node
 * parenthesis, relationship types after a `:` (or `|`) inside brackets, and
 * keywords while typing a bare word. Returns null where completion would be
 * wrong (strings, property maps, comments).
 */
export function analyzeCompletion(text: string, pos: number): GqlCompletionQuery | null {
  const before = text.slice(0, pos);
  const word = WORD_TAIL.exec(before);
  const from = pos - (word?.[0].length ?? 0);
  const context = scanContext(before.slice(0, from));

  if (context === 'blocked') return null;
  if (context !== 'code') {
    const prefix = before.slice(0, from);
    if (/[:|]\s*$/.test(prefix)) {
      if (context === 'paren' && /:\s*$/.test(prefix)) return { kind: 'label', from };
      if (context === 'bracket') return { kind: 'relationshipType', from };
      return null;
    }
  }
  if (word === null) return null;
  if (/[:|.]\s*$/.test(before.slice(0, from))) return null;
  return { kind: 'keyword', from };
}

type ScanContext = 'code' | 'paren' | 'bracket' | 'blocked';

/** Reports the innermost open bracket before `pos`, treating strings, backtick identifiers, and comments as blocked regions. */
function scanContext(text: string): ScanContext {
  const stack: ('paren' | 'bracket' | 'brace')[] = [];
  let index = 0;

  while (index < text.length) {
    const character = text[index];
    const next = text[index + 1];

    if (character === "'" || character === '"' || character === '`') {
      const end = closingQuote(text, index);
      if (end === -1) return 'blocked';
      index = end + 1;
      continue;
    }
    if ((character === '/' && next === '/') || (character === '-' && next === '-')) {
      const end = text.indexOf('\n', index + 2);
      if (end === -1) return 'blocked';
      index = end + 1;
      continue;
    }
    if (character === '/' && next === '*') {
      const end = text.indexOf('*/', index + 2);
      if (end === -1) return 'blocked';
      index = end + 2;
      continue;
    }

    if (character === '(') stack.push('paren');
    else if (character === '[') stack.push('bracket');
    else if (character === '{') stack.push('brace');
    else if (character === ')' || character === ']' || character === '}') stack.pop();
    index += 1;
  }

  const innermost = stack.at(-1);
  if (innermost === undefined) return 'code';
  if (innermost === 'brace') return 'blocked';
  return innermost;
}

function closingQuote(text: string, start: number): number {
  const quote = text[start];
  for (let index = start + 1; index < text.length; index += 1) {
    if (quote !== '`' && text[index] === '\\') {
      index += 1;
      continue;
    }
    if (text[index] === quote) {
      if (text[index + 1] === quote) {
        index += 1;
        continue;
      }
      return index;
    }
  }
  return -1;
}

export function completionOptions(kind: GqlCompletionKind, schema: ObservedSchema): Completion[] {
  if (kind === 'label') {
    return Object.keys(schema.labels).map((label) => schemaCompletion(label, 'class', 'label'));
  }
  if (kind === 'relationshipType') {
    return Object.keys(schema.relationshipTypes).map((type) =>
      schemaCompletion(type, 'type', 'relationship type'),
    );
  }
  return KEYWORDS.map((keyword) => ({ label: keyword, type: 'keyword', boost: -1 }));
}

function schemaCompletion(name: string, type: string, detail: string): Completion {
  return {
    label: name,
    type,
    detail,
    ...(SAFE_IDENTIFIER.test(name) ? {} : { apply: escapeGqlIdentifier(name) }),
  };
}

export function createGqlCompletionSource(getSchema: () => ObservedSchema): CompletionSource {
  return (context: CompletionContext): CompletionResult | null => {
    const query = analyzeCompletion(context.state.doc.toString(), context.pos);
    if (query === null) return null;
    if (query.kind === 'keyword' && !context.explicit && context.pos === query.from) return null;

    const options = completionOptions(query.kind, getSchema());
    if (options.length === 0) return null;

    return {
      from: query.from,
      options,
      validFor: /^[A-Za-z0-9_$]*$/,
    };
  };
}
