import type { ExecutionMode } from '../types';

export type RelationshipDirection = 'outgoing' | 'incoming' | 'undirected';

export interface QueryNodeShape {
  variable?: string;
  labels: string[];
}

export interface QueryRelationshipShape {
  variable?: string;
  types: string[];
  direction: RelationshipDirection;
}

export interface QueryPatternShape {
  nodes: QueryNodeShape[];
  relationships: QueryRelationshipShape[];
}

export interface NamedPathShape {
  alias: string;
  nodes: (string | undefined)[];
  relationships: (string | undefined)[];
  directions: RelationshipDirection[];
}

export interface ReturnShape {
  source: string;
  alias: string;
}

export interface QueryShape {
  ambiguous: boolean;
  patterns: QueryPatternShape[];
  paths: NamedPathShape[];
  returns: ReturnShape[];
}

type TokenKind = 'word' | 'identifier' | 'string' | 'symbol';

interface Token {
  kind: TokenKind;
  text: string;
  parenDepth: number;
  bracketDepth: number;
  braceDepth: number;
}

interface Tokenization {
  ok: boolean;
  tokens: Token[];
}

const WRITE_KEYWORDS = new Set([
  'ALTER',
  'CREATE',
  'DELETE',
  'DENY',
  'DETACH',
  'DROP',
  'GRANT',
  'INSERT',
  'MERGE',
  'REMOVE',
  'REPLACE',
  'REVOKE',
  'SET',
]);

const MATCH_BOUNDARIES = new Set([
  'ALTER',
  'CALL',
  'CREATE',
  'DELETE',
  'DROP',
  'FINISH',
  'INSERT',
  'LIMIT',
  'MATCH',
  'OFFSET',
  'OPTIONAL',
  'ORDER',
  'REMOVE',
  'RETURN',
  'SET',
  'SKIP',
  'UNION',
  'UNWIND',
  'WHERE',
  'WITH',
]);

const RETURN_BOUNDARIES = new Set(['FINISH', 'LIMIT', 'OFFSET', 'ORDER', 'SKIP', 'UNION']);

const emptyShape = (): QueryShape => ({
  ambiguous: true,
  patterns: [],
  paths: [],
  returns: [],
});

function isIdentifier(token: Token | undefined): token is Token {
  return (
    token !== undefined &&
    (token.kind === 'identifier' ||
      (token.kind === 'word' && /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(token.text)))
  );
}

function isTopLevel(token: Token): boolean {
  return token.parenDepth === 0 && token.bracketDepth === 0 && token.braceDepth === 0;
}

function tokenize(gql: string): Tokenization {
  const tokens: Token[] = [];
  let parenDepth = 0;
  let bracketDepth = 0;
  let braceDepth = 0;
  let index = 0;

  const push = (kind: TokenKind, text: string) => {
    tokens.push({ kind, text, parenDepth, bracketDepth, braceDepth });
  };

  while (index < gql.length) {
    const character = gql[index];
    const next = gql[index + 1];

    if (/\s/.test(character)) {
      index += 1;
      continue;
    }

    if ((character === '/' && next === '/') || (character === '-' && next === '-')) {
      index += 2;
      while (index < gql.length && gql[index] !== '\n') index += 1;
      continue;
    }

    if (character === '/' && next === '*') {
      const end = gql.indexOf('*/', index + 2);
      if (end === -1) return { ok: false, tokens: [] };
      index = end + 2;
      continue;
    }

    if (character === "'" || character === '"') {
      const quote = character;
      let value = '';
      let closed = false;
      index += 1;

      while (index < gql.length) {
        if (gql[index] === '\\' && index + 1 < gql.length) {
          value += gql[index + 1];
          index += 2;
          continue;
        }
        if (gql[index] === quote && gql[index + 1] === quote) {
          value += quote;
          index += 2;
          continue;
        }
        if (gql[index] === quote) {
          closed = true;
          index += 1;
          break;
        }
        value += gql[index];
        index += 1;
      }

      if (!closed) return { ok: false, tokens: [] };
      push('string', value);
      continue;
    }

    if (character === '`') {
      let value = '';
      let closed = false;
      index += 1;

      while (index < gql.length) {
        if (gql[index] === '`' && gql[index + 1] === '`') {
          value += '`';
          index += 2;
          continue;
        }
        if (gql[index] === '`') {
          closed = true;
          index += 1;
          break;
        }
        value += gql[index];
        index += 1;
      }

      if (!closed) return { ok: false, tokens: [] };
      push('identifier', value);
      continue;
    }

    if (/[A-Za-z0-9_$]/.test(character)) {
      const start = index;
      index += 1;
      while (index < gql.length && /[A-Za-z0-9_$]/.test(gql[index])) index += 1;
      push('word', gql.slice(start, index));
      continue;
    }

    push('symbol', character);
    index += 1;

    if (character === '(') parenDepth += 1;
    if (character === '[') bracketDepth += 1;
    if (character === '{') braceDepth += 1;
    if (character === ')') parenDepth -= 1;
    if (character === ']') bracketDepth -= 1;
    if (character === '}') braceDepth -= 1;

    if (parenDepth < 0 || bracketDepth < 0 || braceDepth < 0) {
      return { ok: false, tokens: [] };
    }
  }

  if (parenDepth !== 0 || bracketDepth !== 0 || braceDepth !== 0) {
    return { ok: false, tokens: [] };
  }

  return { ok: true, tokens };
}

export function classifyGql(gql: string): ExecutionMode {
  const tokenization = tokenize(gql);

  if (!tokenization.ok) return 'read';

  return tokenization.tokens.some(
    (token) =>
      token.kind === 'word' && isTopLevel(token) && WRITE_KEYWORDS.has(token.text.toUpperCase()),
  )
    ? 'write'
    : 'read';
}

function matchingClose(
  tokens: Token[],
  start: number,
  open: '(' | '[' | '{',
  close: ')' | ']' | '}',
): number {
  const depthKey = open === '(' ? 'parenDepth' : open === '[' ? 'bracketDepth' : 'braceDepth';
  const expectedDepth = tokens[start][depthKey] + 1;

  for (let index = start + 1; index < tokens.length; index += 1) {
    if (tokens[index].text === close && tokens[index][depthKey] === expectedDepth) {
      return index;
    }
  }

  return -1;
}

function parseNode(tokens: Token[], start: number): { node: QueryNodeShape; end: number } | null {
  const end = matchingClose(tokens, start, '(', ')');
  if (end === -1) return null;

  const open = tokens[start];
  const node: QueryNodeShape = { labels: [] };
  let index = start + 1;
  const isAtNodeLevel = (token: Token | undefined) =>
    token !== undefined &&
    token.parenDepth === open.parenDepth + 1 &&
    token.bracketDepth === open.bracketDepth &&
    token.braceDepth === open.braceDepth;

  if (isIdentifier(tokens[index]) && isAtNodeLevel(tokens[index])) {
    node.variable = tokens[index].text;
    index += 1;
  }

  while (tokens[index]?.text === ':' && isAtNodeLevel(tokens[index])) {
    if (!isIdentifier(tokens[index + 1]) || !isAtNodeLevel(tokens[index + 1])) return null;
    node.labels.push(tokens[index + 1].text);
    index += 2;
  }

  if (tokens[index]?.text === '{' && isAtNodeLevel(tokens[index])) {
    const propertyEnd = matchingClose(tokens, index, '{', '}');
    if (propertyEnd === -1 || propertyEnd >= end) return null;
    index = propertyEnd + 1;
  }

  return index === end ? { node, end } : null;
}

function parseRelationship(
  tokens: Token[],
  start: number,
  end: number,
): QueryRelationshipShape | null {
  const bracketStart = tokens.findIndex(
    (token, index) => index >= start && index < end && token.text === '[',
  );
  let bracketEnd = -1;
  let variable: string | undefined;
  const types: string[] = [];

  if (bracketStart !== -1) {
    bracketEnd = matchingClose(tokens, bracketStart, '[', ']');
    if (bracketEnd === -1 || bracketEnd >= end) return null;

    const open = tokens[bracketStart];
    let index = bracketStart + 1;
    const isAtRelationshipLevel = (token: Token | undefined) =>
      token !== undefined &&
      token.bracketDepth === open.bracketDepth + 1 &&
      token.braceDepth === open.braceDepth;

    if (isIdentifier(tokens[index]) && isAtRelationshipLevel(tokens[index])) {
      variable = tokens[index].text;
      index += 1;
    }

    if (tokens[index]?.text === ':' && isAtRelationshipLevel(tokens[index])) {
      do {
        if (!isIdentifier(tokens[index + 1]) || !isAtRelationshipLevel(tokens[index + 1])) {
          return null;
        }
        types.push(tokens[index + 1].text);
        index += 2;
      } while (tokens[index]?.text === '|' && isAtRelationshipLevel(tokens[index]));
    }

    if (index !== bracketEnd) return null;
  }

  const prefix = tokens
    .slice(start, bracketStart === -1 ? end : bracketStart)
    .map((token) => token.text)
    .join('');
  const suffix =
    bracketStart === -1
      ? ''
      : tokens
          .slice(bracketEnd + 1, end)
          .map((token) => token.text)
          .join('');

  let direction: RelationshipDirection;
  if ((bracketStart === -1 && prefix === '<--') || (prefix === '<-' && suffix === '-')) {
    direction = 'incoming';
  } else if ((bracketStart === -1 && prefix === '-->') || (prefix === '-' && suffix === '->')) {
    direction = 'outgoing';
  } else if ((bracketStart === -1 && prefix === '--') || (prefix === '-' && suffix === '-')) {
    direction = 'undirected';
  } else {
    return null;
  }

  return {
    variable,
    types,
    direction,
  };
}

function parsePattern(
  tokens: Token[],
): { pattern: QueryPatternShape; path?: NamedPathShape } | null {
  let index = 0;
  let alias: string | undefined;

  if (isIdentifier(tokens[0]) && tokens[1]?.text === '=') {
    alias = tokens[0].text;
    index = 2;
  }

  if (tokens[index]?.text !== '(') return null;

  const nodes: QueryNodeShape[] = [];
  const relationships: QueryRelationshipShape[] = [];
  let parsedNode = parseNode(tokens, index);
  if (!parsedNode) return null;
  nodes.push(parsedNode.node);
  index = parsedNode.end + 1;

  while (index < tokens.length) {
    const nextNode = tokens.findIndex(
      (token, candidate) => candidate >= index && token.text === '(' && token.parenDepth === 0,
    );
    if (nextNode === -1) return null;

    const relationship = parseRelationship(tokens, index, nextNode);
    if (!relationship) return null;
    relationships.push(relationship);

    parsedNode = parseNode(tokens, nextNode);
    if (!parsedNode) return null;
    nodes.push(parsedNode.node);
    index = parsedNode.end + 1;
  }

  const pattern = { nodes, relationships };
  if (!alias) return { pattern };

  return {
    pattern,
    path: {
      alias,
      nodes: nodes.map((node) => node.variable),
      relationships: relationships.map((relationship) => relationship.variable),
      directions: relationships.map((relationship) => relationship.direction),
    },
  };
}

function splitTopLevel(tokens: Token[]): Token[][] {
  const parts: Token[][] = [];
  let start = 0;

  for (let index = 0; index < tokens.length; index += 1) {
    if (tokens[index].text === ',' && isTopLevel(tokens[index])) {
      parts.push(tokens.slice(start, index));
      start = index + 1;
    }
  }

  parts.push(tokens.slice(start));
  return parts;
}

function parseReturns(tokens: Token[]): ReturnShape[] | null {
  if (tokens.length === 0) return null;

  const returns: ReturnShape[] = [];
  for (const projection of splitTopLevel(tokens)) {
    if (!isIdentifier(projection[0])) return null;

    if (projection.length === 1) {
      returns.push({ source: projection[0].text, alias: projection[0].text });
      continue;
    }

    if (
      projection.length === 3 &&
      projection[1].kind === 'word' &&
      projection[1].text.toUpperCase() === 'AS' &&
      isIdentifier(projection[2])
    ) {
      returns.push({ source: projection[0].text, alias: projection[2].text });
      continue;
    }

    return null;
  }

  return returns;
}

function clauseEnd(tokens: Token[], start: number, boundaries: Set<string>): number {
  for (let index = start; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token.kind === 'word' && isTopLevel(token) && boundaries.has(token.text.toUpperCase())) {
      return index;
    }
  }

  return tokens.length;
}

export function extractQueryShape(gql: string): QueryShape {
  const tokenization = tokenize(gql);
  if (!tokenization.ok) return emptyShape();

  const { tokens } = tokenization;
  const patterns: QueryPatternShape[] = [];
  const paths: NamedPathShape[] = [];
  let foundMatch = false;

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token.kind !== 'word' || !isTopLevel(token) || token.text.toUpperCase() !== 'MATCH') {
      continue;
    }

    foundMatch = true;
    const end = clauseEnd(tokens, index + 1, MATCH_BOUNDARIES);
    const patternTokens = tokens.slice(index + 1, end);

    for (const part of splitTopLevel(patternTokens)) {
      const parsed = parsePattern(part);
      if (!parsed) return emptyShape();
      patterns.push(parsed.pattern);
      if (parsed.path) paths.push(parsed.path);
    }

    index = end - 1;
  }

  if (!foundMatch) return emptyShape();

  const returnIndex = tokens.findIndex(
    (token) => token.kind === 'word' && isTopLevel(token) && token.text.toUpperCase() === 'RETURN',
  );
  const returns =
    returnIndex === -1
      ? []
      : parseReturns(
          tokens.slice(returnIndex + 1, clauseEnd(tokens, returnIndex + 1, RETURN_BOUNDARIES)),
        );

  if (returns === null) return emptyShape();

  return { ambiguous: false, patterns, paths, returns };
}
