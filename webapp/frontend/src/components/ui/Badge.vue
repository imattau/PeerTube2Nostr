<script setup lang="ts">
interface Props {
  variant?: 'success' | 'error' | 'warning' | 'info'
}
withDefaults(defineProps<Props>(), {
  variant: 'info'
})
</script>

<template>
  <span 
    :class="[
      'inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider border',
      variant === 'success' ? 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20' : '',
      variant === 'error' ? 'bg-red-500/10 text-red-500 border-red-500/20' : '',
      variant === 'warning' ? 'bg-amber-500/10 text-amber-500 border-amber-500/20' : '',
      variant === 'info' ? 'bg-indigo-500/10 text-indigo-400 border-indigo-500/20' : ''
    ]"
  >
    <slot />
  </span>
</template>
