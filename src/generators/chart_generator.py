"""
Chart Generator - Generate charts and visualizations
"""
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime
from src.generators.base_generator import BaseGenerator
from src.core.config import settings


# Set style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")


class ChartGenerator(BaseGenerator):
    """
    Generator for charts and visualizations
    """

    def __init__(self, output_dir: Optional[Path] = None,
                figsize: Tuple[int, int] = (10, 6), dpi: int = 100):
        """
        Initialize Chart generator

        Args:
            output_dir: Output directory
            figsize: Default figure size
            dpi: Resolution
        """
        super().__init__(output_dir or settings.OUTPUT_DIR)
        self.figsize = figsize
        self.dpi = dpi
        
        # Color palette
        self.colors = ['#4472C4', '#ED7D31', '#A5A5A5', '#FFC000', '#5B9BD5',
                      '#70AD47', '#264478', '#9E480E', '#636363', '#997300']

    def generate(self, data: pd.DataFrame, filename: str, 
                chart_type: str = 'bar', **kwargs) -> Path:
        """
        Generate chart

        Args:
            data: DataFrame with chart data
            filename: Output filename
            chart_type: Type of chart
            **kwargs: Chart options

        Returns:
            Path to generated file
        """
        if not self.validate_data(data):
            raise ValueError("Invalid data provided")

        filename = self._prepare_filename(filename, '.png')
        output_path = self._get_output_path(filename)

        # Create chart based on type
        if chart_type == 'bar':
            self._create_bar_chart(data, output_path, **kwargs)
        elif chart_type == 'line':
            self._create_line_chart(data, output_path, **kwargs)
        elif chart_type == 'pie':
            self._create_pie_chart(data, output_path, **kwargs)
        elif chart_type == 'scatter':
            self._create_scatter_chart(data, output_path, **kwargs)
        elif chart_type == 'heatmap':
            self._create_heatmap(data, output_path, **kwargs)
        elif chart_type == 'histogram':
            self._create_histogram(data, output_path, **kwargs)
        else:
            raise ValueError(f"Unknown chart type: {chart_type}")

        self.generated_files.append(output_path)
        self.logger.info(f"Chart generated: {output_path}")

        return output_path

    def _create_bar_chart(self, data: pd.DataFrame, output_path: Path, **kwargs) -> None:
        """Create bar chart"""
        fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)

        x = kwargs.get('x', data.columns[0])
        y = kwargs.get('y', data.columns[1])
        title = kwargs.get('title', 'Bar Chart')
        xlabel = kwargs.get('xlabel', x)
        ylabel = kwargs.get('ylabel', y)
        horizontal = kwargs.get('horizontal', False)

        if horizontal:
            ax.barh(data[x], data[y], color=self.colors[0])
        else:
            ax.bar(data[x], data[y], color=self.colors[0])

        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(output_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()

    def _create_line_chart(self, data: pd.DataFrame, output_path: Path, **kwargs) -> None:
        """Create line chart"""
        fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)

        x = kwargs.get('x', data.columns[0])
        y_columns = kwargs.get('y', [data.columns[1]])
        title = kwargs.get('title', 'Line Chart')
        xlabel = kwargs.get('xlabel', x)
        ylabel = kwargs.get('ylabel', 'Value')

        if isinstance(y_columns, str):
            y_columns = [y_columns]

        for i, y in enumerate(y_columns):
            ax.plot(data[x], data[y], marker='o', color=self.colors[i % len(self.colors)],
                   label=y, linewidth=2, markersize=6)

        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        
        if len(y_columns) > 1:
            ax.legend()
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(output_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()

    def _create_pie_chart(self, data: pd.DataFrame, output_path: Path, **kwargs) -> None:
        """Create pie chart"""
        fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)

        labels = kwargs.get('labels', data.columns[0])
        values = kwargs.get('values', data.columns[1])
        title = kwargs.get('title', 'Pie Chart')
        explode = kwargs.get('explode', None)

        colors = self.colors[:len(data)]
        
        wedges, texts, autotexts = ax.pie(
            data[values],
            labels=data[labels],
            colors=colors,
            autopct='%1.1f%%',
            explode=explode,
            startangle=90,
            pctdistance=0.85
        )

        # Style
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')

        ax.set_title(title, fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()

    def _create_scatter_chart(self, data: pd.DataFrame, output_path: Path, **kwargs) -> None:
        """Create scatter chart"""
        fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)

        x = kwargs.get('x', data.columns[0])
        y = kwargs.get('y', data.columns[1])
        title = kwargs.get('title', 'Scatter Plot')
        xlabel = kwargs.get('xlabel', x)
        ylabel = kwargs.get('ylabel', y)
        size = kwargs.get('size', 50)
        color_by = kwargs.get('color_by')

        if color_by and color_by in data.columns:
            scatter = ax.scatter(data[x], data[y], c=data[color_by], 
                               s=size, cmap='viridis', alpha=0.7)
            plt.colorbar(scatter, label=color_by)
        else:
            ax.scatter(data[x], data[y], s=size, color=self.colors[0], alpha=0.7)

        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()

    def _create_heatmap(self, data: pd.DataFrame, output_path: Path, **kwargs) -> None:
        """Create heatmap"""
        fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)

        title = kwargs.get('title', 'Heatmap')
        cmap = kwargs.get('cmap', 'YlOrRd')
        annot = kwargs.get('annot', True)

        sns.heatmap(data, annot=annot, cmap=cmap, ax=ax, fmt='.2f')
        
        ax.set_title(title, fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()

    def _create_histogram(self, data: pd.DataFrame, output_path: Path, **kwargs) -> None:
        """Create histogram"""
        fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)

        column = kwargs.get('column', data.columns[0])
        title = kwargs.get('title', 'Histogram')
        bins = kwargs.get('bins', 20)
        xlabel = kwargs.get('xlabel', column)

        ax.hist(data[column], bins=bins, color=self.colors[0], 
               edgecolor='white', alpha=0.7)
        
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel(xlabel)
        ax.set_ylabel('Frequency')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()

    def create_multi_chart(self, charts: List[Dict], filename: str,
                          layout: Tuple[int, int] = None) -> Path:
        """
        Create figure with multiple charts

        Args:
            charts: List of chart configurations
            filename: Output filename
            layout: Grid layout (rows, cols)

        Returns:
            Path to generated file
        """
        n_charts = len(charts)
        
        if layout:
            rows, cols = layout
        else:
            cols = min(2, n_charts)
            rows = (n_charts + cols - 1) // cols

        fig, axes = plt.subplots(rows, cols, figsize=(self.figsize[0] * cols, 
                                                       self.figsize[1] * rows), dpi=self.dpi)
        
        if n_charts == 1:
            axes = [axes]
        else:
            axes = axes.flatten()

        for i, chart_config in enumerate(charts):
            if i >= len(axes):
                break
                
            ax = axes[i]
            data = chart_config.get('data')
            chart_type = chart_config.get('type', 'bar')
            title = chart_config.get('title', f'Chart {i+1}')

            if chart_type == 'bar':
                x = chart_config.get('x', data.columns[0])
                y = chart_config.get('y', data.columns[1])
                ax.bar(data[x], data[y], color=self.colors[i % len(self.colors)])
            elif chart_type == 'line':
                x = chart_config.get('x', data.columns[0])
                y = chart_config.get('y', data.columns[1])
                ax.plot(data[x], data[y], marker='o', color=self.colors[i % len(self.colors)])
            
            ax.set_title(title, fontsize=12, fontweight='bold')
            ax.tick_params(axis='x', rotation=45)

        # Hide empty subplots
        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)

        filename = self._prepare_filename(filename, '.png')
        output_path = self._get_output_path(filename)

        plt.tight_layout()
        plt.savefig(output_path, dpi=self.dpi, bbox_inches='tight')
        plt.close()

        self.generated_files.append(output_path)
        return output_path

    def set_style(self, style: str) -> None:
        """Set matplotlib style"""
        plt.style.use(style)

    def set_colors(self, colors: List[str]) -> None:
        """Set color palette"""
        self.colors = colors
