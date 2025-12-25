<script setup lang="ts">
interface Props {
  title: string
  value: string | number
  sublabel?: string
}
defineProps<Props>()
</script>

<template>
  <div class="bg-surface-1 p-5 rounded-lg transition-all hover:bg-surface-2 group">
    <p class="text-[12px] font-semibold text-text-muted uppercase tracking-wider mb-1">{{ title }}</p>
    <div class="flex items-baseline gap-2">
      <p class="text-3xl font-bold text-text-primary tracking-tight">{{ value }}</p>
      <span v-if="sublabel" class="text-xs text-text-muted font-medium">{{ sublabel }}</span>
    </div>
  </div>
</template>
