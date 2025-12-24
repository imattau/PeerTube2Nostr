<script setup lang="ts">
interface Props {
  title: string
  icon?: any
}
defineProps<Props>()
</script>

<template>
  <div class="bg-white/[0.02] rounded-xl flex flex-col overflow-hidden">
    <div class="px-5 py-4 flex items-center justify-between border-b border-white/[0.04] bg-white/[0.01]">
      <div class="flex items-center gap-2.5">
        <component :is="icon" v-if="icon" class="w-4 h-4 text-slate-500" />
        <h3 class="text-[15px] font-semibold text-white tracking-tight">{{ title }}</h3>
      </div>
      <div class="flex items-center gap-2">
        <slot name="actions" />
      </div>
    </div>
    <div class="p-5 flex-1">
      <slot />
    </div>
    <div v-if="$slots.footer" class="px-5 py-3 bg-white/[0.01] border-t border-white/[0.04]">
      <slot name="footer" />
    </div>
  </div>
</template>