<script setup lang="ts">
interface Props {
  title: string
  message?: string
  icon?: any
}
defineProps<Props>()
</script>

<template>
  <div class="flex flex-col items-center justify-center py-8 px-4 text-center min-h-[100px]">
    <component :is="icon" v-if="icon" class="w-6 h-6 text-slate-700 mb-2" />
    <p class="text-sm font-semibold text-slate-400">{{ title }}</p>
    <p v-if="message" class="text-xs text-slate-600 mt-1">{{ message }}</p>
    <div v-if="$slots.default" class="mt-4">
      <slot />
    </div>
  </div>
</template>
