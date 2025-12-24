<script setup lang="ts">
interface Props {
  variant?: 'primary' | 'secondary' | 'ghost' | 'danger'
  size?: 'sm' | 'md' | 'lg' | 'icon'
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
      'inline-flex items-center justify-center rounded-md font-medium transition-all active:scale-[0.98] disabled:opacity-50 disabled:active:scale-100',
      variant === 'primary' ? 'bg-indigo-600 text-white hover:bg-indigo-500 shadow-sm shadow-indigo-500/20' : '',
      variant === 'secondary' ? 'bg-white/5 text-slate-300 hover:bg-white/10' : '',
      variant === 'ghost' ? 'text-slate-500 hover:text-slate-200 hover:bg-white/5' : '',
      variant === 'danger' ? 'bg-red-500/10 text-red-500 hover:bg-red-500/20' : '',
      size === 'sm' ? 'px-3 py-1.5 text-xs' : '',
      size === 'md' ? 'px-4 py-2 text-sm' : '',
      size === 'lg' ? 'px-6 py-3 text-base' : '',
      size === 'icon' ? 'p-2' : ''
    ]"
  >
    <slot />
  </button>
</template>
