import type { Tag } from '@transcription/common';

export interface GroupedTag {
  key: string;
  values: string[];
}

// Groups tags by key (first-seen order) so a multi-value key renders as one
// "key: v1, v2" chip.
export function groupTagsByKey(tags: Tag[]): GroupedTag[] {
  const groups: GroupedTag[] = [];
  const byKey = new Map<string, GroupedTag>();
  for (const tag of tags) {
    const existing = byKey.get(tag.key);
    if (existing) {
      existing.values.push(tag.value);
    } else {
      const group: GroupedTag = { key: tag.key, values: [tag.value] };
      byKey.set(tag.key, group);
      groups.push(group);
    }
  }
  return groups;
}
