import type { Tag } from '@transcription/common';

export function formatDiff(
  before: Record<string, unknown> = {},
  after: Record<string, unknown> = {}
): string[] {
  const changes: string[] = [];
  const keys = Array.from(
    new Set([...Object.keys(before), ...Object.keys(after)])
  );

  keys.forEach((key) => {
    // Ignore status since it is highlighted via status chips
    if (key === 'status') {
      return;
    }

    const beforeVal = before[key];
    const afterVal = after[key];

    // Skip if values are equal
    if (JSON.stringify(beforeVal) === JSON.stringify(afterVal)) {
      return;
    }

    if (key === 'tags') {
      const beforeTags = Array.isArray(beforeVal) ? (beforeVal as Tag[]) : [];
      const afterTags = Array.isArray(afterVal) ? (afterVal as Tag[]) : [];
      const beforeStrSet = new Set(
        beforeTags.map((t) => `${t.key}=${t.value}`)
      );
      const afterStrSet = new Set(afterTags.map((t) => `${t.key}=${t.value}`));

      const added = afterTags
        .filter((t) => !beforeStrSet.has(`${t.key}=${t.value}`))
        .map((t) => `"${t.key}=${t.value}"`);
      const removed = beforeTags
        .filter((t) => !afterStrSet.has(`${t.key}=${t.value}`))
        .map((t) => `"${t.key}=${t.value}"`);

      const tagChanges: string[] = [];
      if (added.length > 0) {
        tagChanges.push(`added ${added.join(', ')}`);
      }
      if (removed.length > 0) {
        tagChanges.push(`removed ${removed.join(', ')}`);
      }
      if (tagChanges.length > 0) {
        changes.push(`Tags: ${tagChanges.join(' and ')}`);
      }
      return;
    }

    if (beforeVal === undefined || beforeVal === null) {
      changes.push(`${key} set to "${afterVal}"`);
    } else if (afterVal === undefined || afterVal === null) {
      changes.push(`${key} cleared (was "${beforeVal}")`);
    } else {
      changes.push(`${key} changed from "${beforeVal}" to "${afterVal}"`);
    }
  });

  return changes;
}

export function getEffectiveStatus(
  values: Record<string, unknown> | undefined
): string | undefined {
  if (!values) {
    return undefined;
  }

  const status = values.status;
  if (typeof status !== 'string') {
    return undefined;
  }

  const failureCount = values.failureCount ?? values.failure_count;
  const statusReason = values.statusReason ?? values.status_reason;

  const hasActiveFailure =
    status === 'active' &&
    ((typeof failureCount === 'number' && failureCount > 0) ||
      (typeof statusReason === 'string' && statusReason !== ''));

  return hasActiveFailure ? 'failing' : status;
}
