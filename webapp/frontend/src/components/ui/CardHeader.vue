<script setup lang="ts">
interface Props {
  title: string
  description?: string
  icon?: any
}
defineProps<Props>()
</script>

<template>
  <div class="px-5 py-4 flex items-center justify-between border-b border-border-subtle">
    <div class="flex items-center gap-3">
      <component :is="icon" v-if="icon" class="w-4 h-4 text-text-muted" />
      <div>
        <h3 class="text-sm font-semibold text-text-primary">{{ title }}</h3>
        <p v-if="description" class="text-xs text-text-muted mt-0.5">{{ description }}</p>
      </div>
    </div>
    <div class="flex items-center gap-2">
      <slot name="actions" />
    </div>
  </div>
</template>
