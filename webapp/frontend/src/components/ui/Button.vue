<script setup lang="ts">
interface Props {
  variant?: 'primary' | 'secondary' | 'ghost' | 'danger'
  size?: 'sm' | 'md'
  disabled?: boolean
}
withDefaults(defineProps<Props>(), {
  variant: 'primary',
  size: 'md'
})
</script>

<template>
  <button 
    :disabled="disabled"
    :class="[
      'h-9 inline-flex items-center justify-center rounded-md font-semibold transition-all active:scale-[0.98] disabled:opacity-40 disabled:pointer-events-none',
      variant === 'primary' ? 'bg-accent text-white hover:bg-opacity-90 shadow-sm' : '',
      variant === 'secondary' ? 'bg-surface-2 text-slate-300 hover:bg-white/10' : '',
      variant === 'ghost' ? 'text-slate-500 hover:text-slate-200 hover:bg-surface-2' : '',
      variant === 'danger' ? 'bg-red-500/10 text-red-500 hover:bg-red-500/20' : '',
      size === 'sm' ? 'px-3 text-xs' : '',
      size === 'md' ? 'px-4 text-sm' : '',
    ]"
  >
    <slot />
  </button>
</template>