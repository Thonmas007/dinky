INSERT INTO public.dinky_sys_menu (parent_id, name, path, component, perms, icon, type, display, order_num)
SELECT parent.id, 'K8s 查询', '/registration/cluster/kubernetes-command', './RegCenter/Cluster/KubernetesCommand',
       'registration:cluster:kubernetes-command', 'CodeOutlined', 'C', 0, 33
FROM public.dinky_sys_menu parent
WHERE parent.path = '/registration/cluster'
  AND NOT EXISTS (
      SELECT 1 FROM public.dinky_sys_menu existing
      WHERE existing.path = '/registration/cluster/kubernetes-command'
  );
