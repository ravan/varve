import { classifyGql, extractQueryShape } from './gql';

export type GqlSeverity = 'error' | 'warning';

export interface GqlDiagnostic {
  readonly severity: GqlSeverity;
  readonly message: string;
  /** Character offset where the problem starts. */
  readonly from: number;
  /** Character offset where the problem ends (exclusive, >= from). */
  readonly to: number;
}

const STATEMENT_STARTERS = new Set([
  'ALTER',
  'CALL',
  'CREATE',
  'DELETE',
  'DENY',
  'DETACH',
  'DROP',
  'ERASE',
  'FINISH',
  'FOR',
  'GRANT',
  'INSERT',
  'MATCH',
  'MERGE',
  'OPTIONAL',
  'REMOVE',
  'REPLACE',
  'RETURN',
  'REVOKE',
  'SET',
  'SHOW',
  'UNWIND',
  'USE',
  'WITH',
]);

interface OpenBracket {
  readonly character: string;
  readonly offset: number;
}

const CLOSERS: Record<string, string> = { ')': '(', ']': '[', '}': '{' };
const CLOSER_OF: Record<string, string> = { '(': ')', '[': ']', '{': '}' };

/**
 * Validates a GQL draft well enough to catch what Varve would reject at parse
 * time, with positions for editor squiggles. Errors block submission;
 * warnings flag queries that parse but almost certainly return nothing.
 */
export function validateGql(gql: string): GqlDiagnostic[] {
  if (gql.trim().length === 0) {
    return [
      {
        severity: 'error',
        message: 'Enter a GQL statement.',
        from: 0,
        to: gql.length,
      },
    ];
  }

  const diagnostics: GqlDiagnostic[] = [];
  const stack: OpenBracket[] = [];
  let firstWord: { text: string; from: number; to: number } | null = null;
  let previousMeaningful = '';
  let index = 0;

  while (index < gql.length) {
    const character = gql[index];
    const next = gql[index + 1];

    if (/\s/.test(character)) {
      index += 1;
      continue;
    }

    if ((character === '/' && next === '/') || (character === '-' && next === '-')) {
      const end = gql.indexOf('\n', index + 2);
      index = end === -1 ? gql.length : end + 1;
      continue;
    }

    if (character === '/' && next === '*') {
      const end = gql.indexOf('*/', index + 2);
      if (end === -1) {
        diagnostics.push({
          severity: 'error',
          message: 'Unterminated block comment: expected */.',
          from: index,
          to: gql.length,
        });
        break;
      }
      index = end + 2;
      continue;
    }

    if (character === "'" || character === '"') {
      const end = closingQuote(gql, index);
      if (end === -1) {
        diagnostics.push({
          severity: 'error',
          message: `Unterminated string: expected a closing ${character}.`,
          from: index,
          to: gql.length,
        });
        break;
      }
      previousMeaningful = 'string';
      index = end + 1;
      continue;
    }

    if (character === '`') {
      const end = closingQuote(gql, index);
      diagnostics.push({
        severity: 'error',
        message: 'Varve GQL has no backtick-quoted identifiers; use a plain name.',
        from: index,
        to: end === -1 ? gql.length : end + 1,
      });
      if (end === -1) break;
      previousMeaningful = 'identifier';
      index = end + 1;
      continue;
    }

    if (character === '(' || character === '[' || character === '{') {
      if (character === '[' && previousMeaningful === '-') {
        const relationshipEnd = matchingBracketEnd(gql, index);
        if (relationshipEnd !== -1 && !gql.slice(index + 1, relationshipEnd).includes(':')) {
          diagnostics.push({
            severity: 'error',
            message: 'Varve requires a relationship type, for example -[r:KNOWS]->.',
            from: index,
            to: relationshipEnd + 1,
          });
        }
      }
      stack.push({ character, offset: index });
      previousMeaningful = character;
      index += 1;
      continue;
    }

    if (character === ')' || character === ']' || character === '}') {
      const open = stack.pop();
      if (open === undefined || open.character !== CLOSERS[character]) {
        diagnostics.push({
          severity: 'error',
          message:
            open === undefined
              ? `Unexpected ${character} with no matching ${CLOSERS[character]}.`
              : `Expected ${CLOSER_OF[open.character]} before this ${character}.`,
          from: index,
          to: index + 1,
        });
        if (open !== undefined) stack.push(open);
      }
      previousMeaningful = character;
      index += 1;
      continue;
    }

    if (/[A-Za-z0-9_$]/.test(character)) {
      const start = index;
      while (index < gql.length && /[A-Za-z0-9_$]/.test(gql[index])) index += 1;
      const text = gql.slice(start, index);
      if (firstWord === null && stack.length === 0) {
        firstWord = { text, from: start, to: index };
      }
      previousMeaningful = 'word';
      continue;
    }

    previousMeaningful = character;
    index += 1;
  }

  for (const open of stack) {
    diagnostics.push({
      severity: 'error',
      message: `Unclosed ${open.character}: expected ${CLOSER_OF[open.character]}.`,
      from: open.offset,
      to: open.offset + 1,
    });
  }

  if (firstWord === null) {
    if (diagnostics.length === 0) {
      diagnostics.push({
        severity: 'error',
        message: 'Enter a GQL statement.',
        from: 0,
        to: gql.length,
      });
    }
  } else if (!STATEMENT_STARTERS.has(firstWord.text.toUpperCase())) {
    diagnostics.push({
      severity: 'error',
      message: `Not a GQL statement: expected MATCH, INSERT, FOR VALID_TIME, …, found "${firstWord.text}".`,
      from: firstWord.from,
      to: firstWord.to,
    });
  }

  if (diagnostics.length === 0) {
    diagnostics.push(...unlabeledPatternWarnings(gql));
  }

  return diagnostics.sort((left, right) => left.from - right.from);
}

export function hasGqlErrors(diagnostics: readonly GqlDiagnostic[]): boolean {
  return diagnostics.some(({ severity }) => severity === 'error');
}

/**
 * Varve v1 scans by label, so a read pattern node that has no label anywhere
 * in the query matches nothing. That parses fine, which makes it exactly the
 * silent-empty-result case worth a warning.
 */
function unlabeledPatternWarnings(gql: string): GqlDiagnostic[] {
  if (classifyGql(gql) !== 'read') return [];
  const shape = extractQueryShape(gql);
  if (shape.ambiguous) return [];

  const labeledVariables = new Set<string>();
  for (const pattern of shape.patterns) {
    for (const node of pattern.nodes) {
      if (node.labels.length > 0 && node.variable !== undefined) {
        labeledVariables.add(node.variable);
      }
    }
  }

  const unlabeled = new Set<string>();
  for (const pattern of shape.patterns) {
    for (const node of pattern.nodes) {
      if (node.labels.length > 0) continue;
      if (node.variable !== undefined && labeledVariables.has(node.variable)) continue;
      unlabeled.add(node.variable === undefined ? 'an unlabeled node' : `(${node.variable})`);
    }
  }

  if (unlabeled.size === 0) return [];
  return [
    {
      severity: 'warning',
      message: `${[...unlabeled].join(', ')} has no label; unlabeled patterns match nothing in Varve v1.`,
      from: 0,
      to: 0,
    },
  ];
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

/** Finds the matching `]` for the `[` at `start`, honoring strings; -1 if unclosed. */
function matchingBracketEnd(text: string, start: number): number {
  let depth = 0;
  for (let index = start; index < text.length; index += 1) {
    const character = text[index];
    if (character === "'" || character === '"' || character === '`') {
      const end = closingQuote(text, index);
      if (end === -1) return -1;
      index = end;
      continue;
    }
    if (character === '[') depth += 1;
    if (character === ']') {
      depth -= 1;
      if (depth === 0) return index;
    }
  }
  return -1;
}
