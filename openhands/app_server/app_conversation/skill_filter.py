# openhands/app_server/app_conversation/skill_filter.py
"""Skill filtering utility to reduce request size and improve performance."""

import logging
from typing import List, Any, Set, Optional
from openhands.sdk.context.skills import Skill

_logger = logging.getLogger(__name__)


class SkillFilter:
    """Utility class to filter skills based on various criteria."""

    # 核心技能 - 这些一定会被保留
    ESSENTIAL_SKILLS: Set[str] = {
        # 开发工具
        'docker', 'git', 'python', 'npm', 'node', 'uv', 'pip',
        'kubernetes', 'ssh', 'jupyter', 'deno', 'swift-linux',

        # 核心功能
        'releasenotes', 'agent-memory', 'readiness-report',
        'code-review', 'codereview-roasted', 'learn-from-code-review',

        # 代码管理
        'github', 'gitlab', 'bitbucket', 'azure-devops', 'linear',
        'github-pr-review',

        # 安全和分析
        'security', 'datadog',

        # 基础工具
        'init', 'add-skill', 'skill-creator', 'onboarding-agent',
        'pdflatex', 'discord', 'notion', 'vercel', 'frontend-design',
        'openhands-api', 'theme-factory',  # 保留主题工厂本身，但过滤它的主题文件
    }

    # 需要过滤的模式
    FILTER_PATTERNS: List[str] = [
        'theme-factory/themes/',      # 主题文件（占空间且不常用）
        '/references/',                # 引用文件（除了重要的）
        'flarglebargle',               # 测试技能
    ]

    # 重要引用 - 即使匹配过滤模式也保留
    IMPORTANT_REFERENCES: Set[str] = {
        'skills/readiness-report/references/criteria',
        'skills/readiness-report/references/maturity-levels',
        'skills/skill-creator/references/output-patterns',
        'skills/skill-creator/references/workflows',
        'skills/discord/references/REFERENCE',
        'skills/openhands-api/references/example_prompt',
        'skills/add-javadoc/references/example',
    }

    # 最大技能大小（字符数）- 超过此大小的技能会被过滤
    MAX_SKILL_SIZE: int = 8000

    @classmethod
    def filter_skills(cls, skills: List[Skill], request_id: str = 'unknown') -> List[Skill]:
        """Filter skills based on configured criteria.

        Args:
            skills: List of Skill objects
            request_id: Request ID for logging

        Returns:
            Filtered list of Skill objects
        """
        if not skills:
            return []

        original_count = len(skills)
        filtered_skills: List[Skill] = []
        removed_count = 0
        removed_by_size = 0
        removed_by_pattern = 0
        kept_essential = 0
        kept_special = 0

        for skill in skills:
            # 获取技能名称
            skill_name = cls._get_skill_name(skill)

            # 获取技能内容
            content = cls._get_skill_content(skill)
            content_size = len(content) if content else 0

            # 记录大技能
            if content_size > cls.MAX_SKILL_SIZE:
                _logger.debug(f"[{request_id}] Large skill detected: {skill_name} ({content_size} chars)")

            # 检查是否为必要技能
            if cls._is_essential(skill_name):
                filtered_skills.append(skill)
                kept_essential += 1
                continue

            # 检查是否为重要引用
            if skill_name in cls.IMPORTANT_REFERENCES:
                filtered_skills.append(skill)
                kept_special += 1
                continue

            # 检查是否需要过滤（基于模式）
            should_filter = False
            for pattern in cls.FILTER_PATTERNS:
                if pattern in skill_name:
                    should_filter = True
                    removed_by_pattern += 1
                    break

            if should_filter:
                removed_count += 1
                continue

            # 检查大小
            if content_size > cls.MAX_SKILL_SIZE:
                removed_count += 1
                removed_by_size += 1
                continue

            # 默认保留
            filtered_skills.append(skill)

        # 记录统计信息
        if removed_count > 0 or kept_essential > 0 or kept_special > 0:
            _logger.info(f"[{request_id}] 📊 Skill filtering summary:")
            _logger.info(f"[{request_id}]   - Original count: {original_count}")
            _logger.info(f"[{request_id}]   - Filtered count: {len(filtered_skills)}")
            _logger.info(f"[{request_id}]   - Removed total: {removed_count}")
            _logger.info(f"[{request_id}]   - Removed by pattern: {removed_by_pattern}")
            _logger.info(f"[{request_id}]   - Removed by size: {removed_by_size}")
            _logger.info(f"[{request_id}]   - Kept essential: {kept_essential}")
            _logger.info(f"[{request_id}]   - Kept special: {kept_special}")

        return filtered_skills

    @classmethod
    def _is_essential(cls, skill_name: str) -> bool:
        """Check if a skill is essential."""
        # 完全匹配
        if skill_name in cls.ESSENTIAL_SKILLS:
            return True

        # 部分匹配（用于处理带路径的技能）
        for essential in cls.ESSENTIAL_SKILLS:
            if essential in skill_name and len(essential) > 3:  # 避免太短的词误匹配
                return True

        return False

    @classmethod
    def _get_skill_name(cls, skill: Skill) -> str:
        """Extract skill name from skill object."""
        return skill.name if hasattr(skill, 'name') else str(skill)

    @classmethod
    def _get_skill_content(cls, skill: Skill) -> str:
        """Extract skill content from skill object."""
        return skill.content if hasattr(skill, 'content') and skill.content else ''
