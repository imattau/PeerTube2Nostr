<script setup lang="ts">
interface Props {
  title: string
  message?: string
  icon?: any
}
defineProps<Props>()
</script>

<template>
  <div class="flex flex-col items-center justify-center py-12 px-4 text-center h-full min-h-[120px]">
    <div class="bg-surface-2 p-3 rounded-full mb-3 border border-border-subtle">
      <component :is="icon" v-if="icon" class="w-5 h-5 text-slate-600" />
    </div>
    <p class="text-[13px] font-semibold text-text-primary">{{ title }}</p>
    <p v-if="message" class="text-xs text-text-muted mt-1 max-w-xs">{{ message }}</p>
    <div v-if="$slots.default" class="mt-4">
      <slot />
    </div>
  </div>
</template>
