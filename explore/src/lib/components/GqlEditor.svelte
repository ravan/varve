<script lang="ts">
  import { createGqlCompletionSource } from '$lib/logic/gql-autocomplete';
  import { hasGqlErrors, validateGql } from '$lib/logic/gql-validate';
  import type { ObservedSchema } from '$lib/logic/schema';
  import { autocompletion } from '@codemirror/autocomplete';
  import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
  import { bracketMatching } from '@codemirror/language';
  import { setDiagnostics } from '@codemirror/lint';
  import { EditorState } from '@codemirror/state';
  import {
    Decoration,
    EditorView,
    keymap,
    lineNumbers as lineNumbersExtension,
    MatchDecorator,
    placeholder as placeholderExtension,
    type DecorationSet,
    type ViewUpdate,
    ViewPlugin,
  } from '@codemirror/view';
  import { onMount } from 'svelte';

  let {
    value,
    onChange,
    onSubmit,
    schema,
    ariaLabel = 'GQL query',
    placeholder,
    lineNumbers = false,
    compact = false,
    onValidation,
  }: {
    value: string;
    onChange: (value: string) => void;
    onSubmit: () => void;
    schema: () => ObservedSchema;
    ariaLabel?: string;
    placeholder?: string;
    lineNumbers?: boolean;
    compact?: boolean;
    onValidation?: (valid: boolean) => void;
  } = $props();

  let editorHost = $state<HTMLDivElement | null>(null);
  let editor = $state<EditorView | null>(null);

  let diagnostics = $derived(validateGql(value));
  let blocked = $derived(hasGqlErrors(diagnostics));
  // The empty-draft error would nag before the user has typed anything; the
  // parents already disable their submit buttons on an empty draft.
  let feedback = $derived(value.trim().length === 0 ? undefined : diagnostics[0]);

  function submit(): void {
    if (!blocked) onSubmit();
  }

  const keywordMatcher = new MatchDecorator({
    regexp:
      /\b(?:ALTER|AND|AS|BY|CALL|CASE|CREATE|DELETE|DENY|DETACH|DISTINCT|DROP|ELSE|END|EXISTS|FALSE|FINISH|FOR|GRANT|INSERT|LIMIT|MATCH|MERGE|NOT|NULL|OFFSET|OPTIONAL|OR|ORDER|REMOVE|REPLACE|RETURN|REVOKE|SET|SKIP|SYSTEM_TIME|THEN|TRUE|UNION|UNWIND|VALID_TIME|WHEN|WHERE|WITH|YIELD)\b/gi,
    decoration: Decoration.mark({ class: 'cm-gql-keyword' }),
  });

  class GqlKeywordHighlighter {
    decorations: DecorationSet;

    constructor(view: EditorView) {
      this.decorations = keywordMatcher.createDeco(view);
    }

    update(update: ViewUpdate): void {
      this.decorations = keywordMatcher.updateDeco(update, this.decorations);
    }
  }

  const gqlKeywordHighlighting = ViewPlugin.fromClass(GqlKeywordHighlighter, {
    decorations: (plugin) => plugin.decorations,
  });

  onMount(() => {
    if (editorHost === null) return;
    editor = new EditorView({
      parent: editorHost,
      state: EditorState.create({
        doc: value,
        extensions: [
          ...(lineNumbers ? [lineNumbersExtension()] : []),
          ...(placeholder === undefined ? [] : [placeholderExtension(placeholder)]),
          bracketMatching(),
          history(),
          gqlKeywordHighlighting,
          autocompletion({ override: [createGqlCompletionSource(schema)] }),
          EditorView.lineWrapping,
          EditorView.contentAttributes.of({
            'aria-label': ariaLabel,
            autocapitalize: 'off',
            autocomplete: 'off',
            spellcheck: 'false',
          }),
          EditorView.updateListener.of((update) => {
            if (update.docChanged) onChange(update.state.doc.toString());
          }),
          keymap.of([
            {
              key: 'Mod-Enter',
              preventDefault: true,
              run: () => {
                submit();
                return true;
              },
            },
            ...defaultKeymap,
            ...historyKeymap,
          ]),
        ],
      }),
    });

    return () => {
      editor?.destroy();
      editor = null;
    };
  });

  $effect(() => {
    const draft = value;
    if (editor !== null && editor.state.doc.toString() !== draft) {
      editor.dispatch({
        changes: { from: 0, to: editor.state.doc.length, insert: draft },
      });
    }
  });

  $effect(() => {
    onValidation?.(!blocked);
  });

  $effect(() => {
    const current = diagnostics;
    if (editor === null) return;
    const docLength = editor.state.doc.length;
    editor.dispatch(
      setDiagnostics(
        editor.state,
        (value.trim().length === 0 ? [] : current).map((diagnostic) => ({
          from: Math.min(diagnostic.from, docLength),
          to: Math.min(Math.max(diagnostic.to, diagnostic.from), docLength),
          severity: diagnostic.severity,
          message: diagnostic.message,
        })),
      ),
    );
  });
</script>

<div
  bind:this={editorHost}
  class={compact ? 'query-editor query-editor-compact min-w-0' : 'query-editor min-h-56 min-w-0'}
></div>
{#if feedback !== undefined}
  <p
    class={feedback.severity === 'error'
      ? 'text-destructive border-t px-3 py-1.5 text-xs'
      : 'text-muted-foreground border-t px-3 py-1.5 text-xs'}
    role={feedback.severity === 'error' ? 'alert' : 'status'}
  >
    {feedback.severity === 'error' ? 'Invalid GQL: ' : 'Hint: '}{feedback.message}
  </p>
{/if}
