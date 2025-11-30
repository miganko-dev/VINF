import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from typing import List, Dict, Optional
import webbrowser
from pathlib import Path
import json
from loguru import logger
from PIL import Image, ImageTk
import requests
from io import BytesIO

from indexer.core.lucene_indexer import LuceneStyleIndexer
from indexer.config import JOINED_DIR

JOINED_DATA_FILE = JOINED_DIR / "pokemon_with_wiki_and_cards.json"


class LuceneSearchGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Pokemon Card Search - Lucene Indexer")
        self.root.geometry("1400x900")
        self.root.minsize(1200, 700)

        self.indexer = LuceneStyleIndexer()
        self.current_results = []
        self.pokemon_wiki_data = {}

        self.setup_styles()
        self.load_pokemon_wiki_data()
        self.create_widgets()
        self.load_index()

    def load_pokemon_wiki_data(self):
        try:
            if JOINED_DATA_FILE.exists():
                with open(JOINED_DATA_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for pokemon in data:
                    name = pokemon.get('pokemon', '').lower()
                    if name:
                        self.pokemon_wiki_data[name] = pokemon
                logger.info(f"Loaded wiki data for {len(self.pokemon_wiki_data)} Pokemon")
            else:
                logger.warning(f"Joined data file not found: {JOINED_DATA_FILE}")
        except Exception as e:
            logger.error(f"Error loading Pokemon wiki data: {e}")

    def get_wiki_info_for_pokemon(self, pokemon_name: str) -> Optional[Dict]:
        if not pokemon_name:
            return None
        name_lower = pokemon_name.lower()
        pokemon_data = self.pokemon_wiki_data.get(name_lower)
        if pokemon_data:
            return pokemon_data.get('wiki_info')
        return None

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('Title.TLabel', font=('Arial', 16, 'bold'), foreground='#2c3e50')
        style.configure('Header.TLabel', font=('Arial', 12, 'bold'), foreground='#34495e')
        style.configure('Info.TLabel', font=('Arial', 10), foreground='#7f8c8d')
        style.configure('Search.TButton', font=('Arial', 11, 'bold'), padding=10)
        style.configure('QueryType.TRadiobutton', font=('Arial', 10))
        style.configure('Results.Treeview', rowheight=30, font=('Arial', 10))
        style.configure('Results.Treeview.Heading', font=('Arial', 11, 'bold'))

    def create_widgets(self):
        top_frame = ttk.Frame(self.root, padding="10")
        top_frame.pack(fill=tk.X)

        ttk.Label(top_frame, text="Lucene Pokemon Card Search",
                  style='Title.TLabel').pack(side=tk.LEFT)

        self.stats_label = ttk.Label(top_frame, text="Loading index...",
                                     style='Info.TLabel')
        self.stats_label.pack(side=tk.RIGHT)

        query_type_frame = ttk.LabelFrame(self.root, text="Query Type", padding="10")
        query_type_frame.pack(fill=tk.X, padx=10, pady=5)

        self.query_type_var = tk.StringVar(value="boolean")

        query_types = [
            ("Boolean (AND/OR)", "boolean", "pokemon:pikachu AND card_set:151"),
            ("Range (Price)", "range", "Find cards by price range"),
            ("Phrase (Exact)", "phrase", "\"reverse holo\""),
            ("Fuzzy (Typos)", "fuzzy", "pikacu -> pikachu"),
            ("Combined", "combined", "Full Lucene syntax")
        ]

        for i, (label, value, desc) in enumerate(query_types):
            frame = ttk.Frame(query_type_frame)
            frame.pack(side=tk.LEFT, padx=15)
            rb = ttk.Radiobutton(frame, text=label, variable=self.query_type_var,
                                 value=value, command=self.on_query_type_change,
                                 style='QueryType.TRadiobutton')
            rb.pack()
            ttk.Label(frame, text=desc, style='Info.TLabel').pack()

        self.search_frame = ttk.LabelFrame(self.root, text="Search", padding="15")
        self.search_frame.pack(fill=tk.X, padx=10, pady=5)

        self.create_boolean_frame()
        self.create_range_frame()
        self.create_phrase_frame()
        self.create_fuzzy_frame()
        self.create_combined_frame()
        self.on_query_type_change()

        results_frame = ttk.LabelFrame(self.root, text="Search Results", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        tree_frame = ttk.Frame(results_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)

        vsb = ttk.Scrollbar(tree_frame, orient="vertical")
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        hsb = ttk.Scrollbar(tree_frame, orient="horizontal")
        hsb.pack(side=tk.BOTTOM, fill=tk.X)

        columns = ('Rank', 'Score', 'Name', 'Pokemon', 'Set', 'Rarity', 'Price', 'Wiki Page')
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings',
                                 yscrollcommand=vsb.set, xscrollcommand=hsb.set,
                                 style='Results.Treeview')
        vsb.config(command=self.tree.yview)
        hsb.config(command=self.tree.xview)

        column_widths = {
            'Rank': 50, 'Score': 80, 'Name': 200, 'Pokemon': 120,
            'Set': 150, 'Rarity': 100, 'Price': 80, 'Wiki Page': 200
        }

        for col in columns:
            self.tree.heading(col, text=col, command=lambda c=col: self.sort_column(c))
            self.tree.column(col, width=column_widths[col], anchor=tk.W)

        self.tree.pack(fill=tk.BOTH, expand=True)
        self.tree.bind('<Double-Button-1>', self.show_details)

        status_frame = ttk.Frame(self.root)
        status_frame.pack(fill=tk.X, side=tk.BOTTOM)
        self.status_label = ttk.Label(status_frame, text="Ready", relief=tk.SUNKEN,
                                      anchor=tk.W, font=('Arial', 9))
        self.status_label.pack(fill=tk.X, padx=5, pady=2)

    def create_boolean_frame(self):
        self.boolean_frame = ttk.Frame(self.search_frame)

        ttk.Label(self.boolean_frame, text="Boolean Query:",
                  font=('Arial', 11)).grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        self.boolean_entry = ttk.Entry(self.boolean_frame, font=('Arial', 11), width=60)
        self.boolean_entry.grid(row=0, column=1, sticky=tk.EW, padx=5, pady=5)
        self.boolean_entry.bind('<Return>', lambda e: self.search())

        ttk.Label(self.boolean_frame, text="Examples: pikachu AND 151, pokemon:charizard OR pokemon:pikachu",
                  style='Info.TLabel').grid(row=1, column=1, sticky=tk.W, padx=5)

        ttk.Label(self.boolean_frame, text="Results:",
                  font=('Arial', 11)).grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)

        self.boolean_topk = tk.IntVar(value=20)
        ttk.Spinbox(self.boolean_frame, from_=5, to=100,
                    textvariable=self.boolean_topk, width=8).grid(row=0, column=3, padx=5)

        ttk.Button(self.boolean_frame, text="Search",
                   command=self.search, style='Search.TButton').grid(row=0, column=4, padx=15)

        self.boolean_frame.columnconfigure(1, weight=1)

    def create_range_frame(self):
        self.range_frame = ttk.Frame(self.search_frame)

        ttk.Label(self.range_frame, text="Price Range ($):",
                  font=('Arial', 11)).grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        ttk.Label(self.range_frame, text="Min:", font=('Arial', 10)).grid(row=0, column=1, padx=5)
        self.min_price_var = tk.DoubleVar(value=0.0)
        ttk.Entry(self.range_frame, textvariable=self.min_price_var,
                  width=10, font=('Arial', 11)).grid(row=0, column=2, padx=5)

        ttk.Label(self.range_frame, text="Max:", font=('Arial', 10)).grid(row=0, column=3, padx=5)
        self.max_price_var = tk.DoubleVar(value=100.0)
        ttk.Entry(self.range_frame, textvariable=self.max_price_var,
                  width=10, font=('Arial', 11)).grid(row=0, column=4, padx=5)

        ttk.Label(self.range_frame, text="Results:",
                  font=('Arial', 11)).grid(row=0, column=5, padx=5)
        self.range_topk = tk.IntVar(value=20)
        ttk.Spinbox(self.range_frame, from_=5, to=100,
                    textvariable=self.range_topk, width=8).grid(row=0, column=6, padx=5)

        ttk.Button(self.range_frame, text="Search",
                   command=self.search, style='Search.TButton').grid(row=0, column=7, padx=15)

        ttk.Label(self.range_frame, text="Find cards within a specific price range (sorted by price)",
                  style='Info.TLabel').grid(row=1, column=1, columnspan=6, sticky=tk.W, padx=5)

    def create_phrase_frame(self):
        self.phrase_frame = ttk.Frame(self.search_frame)

        ttk.Label(self.phrase_frame, text="Exact Phrase:",
                  font=('Arial', 11)).grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        self.phrase_entry = ttk.Entry(self.phrase_frame, font=('Arial', 11), width=40)
        self.phrase_entry.grid(row=0, column=1, sticky=tk.EW, padx=5, pady=5)
        self.phrase_entry.bind('<Return>', lambda e: self.search())

        ttk.Label(self.phrase_frame, text="Field:", font=('Arial', 11)).grid(row=0, column=2, padx=5)
        self.phrase_field_var = tk.StringVar(value="card_name")
        phrase_field_combo = ttk.Combobox(self.phrase_frame, textvariable=self.phrase_field_var,
                                          state='readonly', width=12)
        phrase_field_combo['values'] = ('card_name', 'pokemon', 'card_set', 'content')
        phrase_field_combo.grid(row=0, column=3, padx=5)

        ttk.Label(self.phrase_frame, text="Results:",
                  font=('Arial', 11)).grid(row=0, column=4, padx=5)
        self.phrase_topk = tk.IntVar(value=20)
        ttk.Spinbox(self.phrase_frame, from_=5, to=100,
                    textvariable=self.phrase_topk, width=8).grid(row=0, column=5, padx=5)

        ttk.Button(self.phrase_frame, text="Search",
                   command=self.search, style='Search.TButton').grid(row=0, column=6, padx=15)

        ttk.Label(self.phrase_frame, text="Examples: reverse holo, full art, pokemon card 151",
                  style='Info.TLabel').grid(row=1, column=1, columnspan=5, sticky=tk.W, padx=5)

        self.phrase_frame.columnconfigure(1, weight=1)

    def create_fuzzy_frame(self):
        self.fuzzy_frame = ttk.Frame(self.search_frame)

        ttk.Label(self.fuzzy_frame, text="Fuzzy Term:",
                  font=('Arial', 11)).grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        self.fuzzy_entry = ttk.Entry(self.fuzzy_frame, font=('Arial', 11), width=30)
        self.fuzzy_entry.grid(row=0, column=1, sticky=tk.EW, padx=5, pady=5)
        self.fuzzy_entry.bind('<Return>', lambda e: self.search())

        ttk.Label(self.fuzzy_frame, text="Field:", font=('Arial', 11)).grid(row=0, column=2, padx=5)
        self.fuzzy_field_var = tk.StringVar(value="pokemon")
        fuzzy_field_combo = ttk.Combobox(self.fuzzy_frame, textvariable=self.fuzzy_field_var,
                                         state='readonly', width=12)
        fuzzy_field_combo['values'] = ('pokemon', 'card_name', 'card_set')
        fuzzy_field_combo.grid(row=0, column=3, padx=5)

        ttk.Label(self.fuzzy_frame, text="Max Distance:",
                  font=('Arial', 11)).grid(row=0, column=4, padx=5)
        self.fuzzy_dist_var = tk.IntVar(value=2)
        ttk.Spinbox(self.fuzzy_frame, from_=1, to=3,
                    textvariable=self.fuzzy_dist_var, width=5).grid(row=0, column=5, padx=5)

        ttk.Label(self.fuzzy_frame, text="Results:",
                  font=('Arial', 11)).grid(row=0, column=6, padx=5)
        self.fuzzy_topk = tk.IntVar(value=20)
        ttk.Spinbox(self.fuzzy_frame, from_=5, to=100,
                    textvariable=self.fuzzy_topk, width=8).grid(row=0, column=7, padx=5)

        ttk.Button(self.fuzzy_frame, text="Search",
                   command=self.search, style='Search.TButton').grid(row=0, column=8, padx=15)

        ttk.Label(self.fuzzy_frame, text="Examples: pikacu -> pikachu, charazard -> charizard (typo correction)",
                  style='Info.TLabel').grid(row=1, column=1, columnspan=7, sticky=tk.W, padx=5)

        self.fuzzy_frame.columnconfigure(1, weight=1)

    def create_combined_frame(self):
        self.combined_frame = ttk.Frame(self.search_frame)

        ttk.Label(self.combined_frame, text="Lucene Query:",
                  font=('Arial', 11)).grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

        self.combined_entry = ttk.Entry(self.combined_frame, font=('Arial', 11), width=60)
        self.combined_entry.grid(row=0, column=1, sticky=tk.EW, padx=5, pady=5)
        self.combined_entry.bind('<Return>', lambda e: self.search())

        ttk.Label(self.combined_frame, text="Results:",
                  font=('Arial', 11)).grid(row=0, column=2, padx=5)
        self.combined_topk = tk.IntVar(value=20)
        ttk.Spinbox(self.combined_frame, from_=5, to=100,
                    textvariable=self.combined_topk, width=8).grid(row=0, column=3, padx=5)

        ttk.Button(self.combined_frame, text="Search",
                   command=self.search, style='Search.TButton').grid(row=0, column=4, padx=15)

        ttk.Label(self.combined_frame,
                  text="Supports: AND/OR, field:value, \"phrases\", term~2 (fuzzy), price:[1.0 TO 10.0]",
                  style='Info.TLabel').grid(row=1, column=1, columnspan=3, sticky=tk.W, padx=5)

        self.combined_frame.columnconfigure(1, weight=1)

    def on_query_type_change(self):
        for frame in [self.boolean_frame, self.range_frame, self.phrase_frame,
                      self.fuzzy_frame, self.combined_frame]:
            frame.pack_forget()

        query_type = self.query_type_var.get()
        if query_type == "boolean":
            self.boolean_frame.pack(fill=tk.X)
        elif query_type == "range":
            self.range_frame.pack(fill=tk.X)
        elif query_type == "phrase":
            self.phrase_frame.pack(fill=tk.X)
        elif query_type == "fuzzy":
            self.fuzzy_frame.pack(fill=tk.X)
        elif query_type == "combined":
            self.combined_frame.pack(fill=tk.X)

    def load_index(self):
        try:
            self.status_label.config(text="Loading index...")
            self.root.update()

            if not self.indexer.open_index():
                logger.info("Index not found, building...")
                self.status_label.config(text="Building index (this may take a moment)...")
                self.root.update()
                self.indexer.build_index(use_joined_data=False)
                self.indexer.open_index()

            stats = self.indexer.get_statistics()
            stats_text = f"Documents: {stats.get('total_documents', 0):,} | Fields: {len(stats.get('schema_fields', []))}"
            self.stats_label.config(text=stats_text)
            self.status_label.config(text="Index loaded successfully")
            logger.info("Lucene index loaded successfully")

        except Exception as e:
            error_msg = f"Error loading index: {e}"
            logger.error(error_msg)
            messagebox.showerror("Error", error_msg)
            self.status_label.config(text="Error loading index")

    def search(self):
        try:
            for item in self.tree.get_children():
                self.tree.delete(item)

            query_type = self.query_type_var.get()
            results = []

            if query_type == "boolean":
                query = self.boolean_entry.get().strip()
                if not query:
                    messagebox.showwarning("Warning", "Please enter a search query")
                    return
                self.status_label.config(text=f"Boolean search: '{query}'...")
                self.root.update()
                results = self.indexer.search_boolean(query, top_k=self.boolean_topk.get())

            elif query_type == "range":
                min_price = self.min_price_var.get()
                max_price = self.max_price_var.get()
                self.status_label.config(text=f"Range search: ${min_price} - ${max_price}...")
                self.root.update()
                results = self.indexer.search_range(min_price, max_price, top_k=self.range_topk.get())

            elif query_type == "phrase":
                phrase = self.phrase_entry.get().strip()
                if not phrase:
                    messagebox.showwarning("Warning", "Please enter a phrase")
                    return
                field = self.phrase_field_var.get()
                self.status_label.config(text=f"Phrase search: \"{phrase}\" in {field}...")
                self.root.update()
                results = self.indexer.search_phrase(phrase, field=field, top_k=self.phrase_topk.get())

            elif query_type == "fuzzy":
                term = self.fuzzy_entry.get().strip()
                if not term:
                    messagebox.showwarning("Warning", "Please enter a search term")
                    return
                field = self.fuzzy_field_var.get()
                max_dist = self.fuzzy_dist_var.get()
                self.status_label.config(text=f"Fuzzy search: {term}~{max_dist} in {field}...")
                self.root.update()
                results = self.indexer.search_fuzzy(term, field=field, max_dist=max_dist,
                                                    top_k=self.fuzzy_topk.get())

            elif query_type == "combined":
                query = self.combined_entry.get().strip()
                if not query:
                    messagebox.showwarning("Warning", "Please enter a search query")
                    return
                self.status_label.config(text=f"Combined search: '{query}'...")
                self.root.update()
                results = self.indexer.search_combined(query, top_k=self.combined_topk.get())

            self.current_results = results
            self.display_results(results, query_type)

        except Exception as e:
            error_msg = f"Search error: {e}"
            logger.error(error_msg)
            messagebox.showerror("Error", error_msg)
            self.status_label.config(text="Search failed")

    def display_results(self, results: List[Dict], query_type: str):
        if results:
            for idx, result in enumerate(results, 1):
                price = result.get('price', 0)
                price_str = f"${price:.2f}" if isinstance(price, (int, float)) else str(price)

                values = (
                    idx,
                    f"{result.get('score', 0):.4f}",
                    result.get('card_name', 'N/A'),
                    result.get('pokemon', 'N/A'),
                    result.get('card_set', 'N/A'),
                    result.get('rarity', '-'),
                    price_str,
                    result.get('wiki_page', '-') or '-'
                )
                self.tree.insert('', tk.END, values=values)

            status_msg = f"Found {len(results)} results ({query_type} query)"
            logger.info(status_msg)
        else:
            status_msg = "No results found"
            logger.warning(status_msg)
            messagebox.showinfo("No Results", "No cards found matching your search criteria.")

        self.status_label.config(text=status_msg)

    def sort_column(self, col: str):
        items = [(self.tree.set(item, col), item) for item in self.tree.get_children('')]

        try:
            items.sort(key=lambda x: float(x[0].replace(',', '').replace('$', '').replace('-', '0')))
        except (ValueError, AttributeError):
            items.sort()

        for idx, (val, item) in enumerate(items):
            self.tree.move(item, '', idx)

    def show_details(self, event):
        selection = self.tree.selection()
        if not selection:
            return

        try:
            item = self.tree.item(selection[0])
            values = item['values']
            if not values:
                return

            rank = int(values[0]) - 1
            if rank < len(self.current_results):
                result = self.current_results[rank]
                self.show_card_details(result)
        except Exception as e:
            logger.error(f"Error showing details: {e}")
            messagebox.showerror("Error", f"Could not show card details: {e}")

    def show_card_details(self, card: Dict):
        detail_window = tk.Toplevel(self.root)
        detail_window.title(f"Card Details - {card.get('card_name', 'Unknown')}")
        detail_window.geometry("1200x700")

        main_container = ttk.Frame(detail_window, padding="10")
        main_container.pack(fill=tk.BOTH, expand=True)

        left_frame = ttk.Frame(main_container)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

        title_frame = ttk.Frame(left_frame, padding="10")
        title_frame.pack(fill=tk.X)
        ttk.Label(title_frame, text=card.get('card_name', 'Unknown'),
                  style='Title.TLabel').pack()

        notebook = ttk.Notebook(left_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=5)

        card_tab = ttk.Frame(notebook, padding="10")
        notebook.add(card_tab, text="Card Info")

        card_text = scrolledtext.ScrolledText(card_tab, wrap=tk.WORD,
                                              font=('Courier', 10), height=18)
        card_text.pack(fill=tk.BOTH, expand=True)

        price = card.get('price', 0)
        price_str = f"${price:.2f}" if isinstance(price, (int, float)) else str(price)

        card_info_lines = [
            f"Card Name:    {card.get('card_name', 'N/A')}",
            f"Pokemon:      {card.get('pokemon', 'N/A')}",
            f"Rarity:       {card.get('rarity', '-') or '-'}",
            f"",
            f"Set:          {card.get('card_set', 'N/A')}",
            f"Card ID:      {card.get('card_id', 'N/A')}",
            f"Price:        {price_str}",
            f"",
            f"Wiki Page:    {card.get('wiki_page', '-') or '-'}",
            f"",
            f"Search Score: {card.get('score', 'N/A')}",
            f"Query Type:   {card.get('query_type', 'N/A')}",
            f"",
            f"Image URL:    {card.get('image_url', 'N/A') or 'N/A'}",
        ]

        card_text.insert('1.0', '\n'.join(card_info_lines))
        card_text.config(state='disabled')

        wiki_tab = ttk.Frame(notebook, padding="10")
        notebook.add(wiki_tab, text="Wikipedia Info")

        wiki_text = scrolledtext.ScrolledText(wiki_tab, wrap=tk.WORD,
                                              font=('Courier', 10), height=18)
        wiki_text.pack(fill=tk.BOTH, expand=True)

        pokemon_name = card.get('pokemon', '')
        wiki_info = self.get_wiki_info_for_pokemon(pokemon_name)

        if wiki_info:
            wiki_lines = [f"=== Wikipedia Information for {pokemon_name} ===", ""]

            def fmt(val):
                if val is None:
                    return "N/A"
                if isinstance(val, list):
                    return ", ".join(str(v) for v in val)
                return str(val)

            wiki_lines.append(f"Types:           {fmt(wiki_info.get('types'))}")
            wiki_lines.append(f"Species:         {fmt(wiki_info.get('species'))}")
            wiki_lines.append(f"Generation:      {fmt(wiki_info.get('generation'))}")
            wiki_lines.append(f"Pokedex Number:  {fmt(wiki_info.get('pokedex_number'))}")
            wiki_lines.append("")
            wiki_lines.append(f"Abilities:       {fmt(wiki_info.get('abilities'))}")
            wiki_lines.append("")
            wiki_lines.append(f"Evolves From:    {fmt(wiki_info.get('evolves_from'))}")
            wiki_lines.append(f"Evolves To:      {fmt(wiki_info.get('evolves_to'))}")
            wiki_lines.append("")
            wiki_lines.append(f"Height:          {fmt(wiki_info.get('height'))}")
            wiki_lines.append(f"Weight:          {fmt(wiki_info.get('weight'))}")
            wiki_lines.append("")
            wiki_lines.append(f"Japanese Name:   {fmt(wiki_info.get('japanese_name'))}")
            wiki_lines.append(f"First Game:      {fmt(wiki_info.get('first_game'))}")
            wiki_lines.append(f"Created By:      {fmt(wiki_info.get('created_by'))}")
            wiki_lines.append("")

            design_desc = wiki_info.get('design_description')
            if design_desc:
                wiki_lines.append("=== Design Description ===")
                wiki_lines.append("")
                wrapped = self._wrap_text(design_desc, 70)
                wiki_lines.append(wrapped)
                wiki_lines.append("")

            description = wiki_info.get('description')
            if description:
                wiki_lines.append("=== Description ===")
                wiki_lines.append("")
                wrapped = self._wrap_text(description, 70)
                wiki_lines.append(wrapped)

            wiki_text.insert('1.0', '\n'.join(wiki_lines))
        else:
            wiki_text.insert('1.0', f"No Wikipedia information available for {pokemon_name}.\n\n"
                                    f"This Pokemon may not have been matched with a Wikipedia page.\n"
                                    f"Run the wiki parser join operation to update the data.")

        wiki_text.config(state='disabled')

        btn_frame = ttk.Frame(left_frame, padding="10")
        btn_frame.pack(fill=tk.X)

        image_url = card.get('image_url', '')

        def open_image():
            if image_url:
                webbrowser.open(image_url)

        if image_url:
            ttk.Button(btn_frame, text="Open Image", command=open_image).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Close", command=detail_window.destroy).pack(side=tk.RIGHT, padx=5)

        right_frame = ttk.LabelFrame(main_container, text="Card Image", padding="10")
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH)

        if image_url:
            try:
                response = requests.get(image_url, timeout=5)
                if response.status_code == 200:
                    image_data = BytesIO(response.content)
                    pil_image = Image.open(image_data)

                    max_width = 300
                    max_height = 450
                    pil_image.thumbnail((max_width, max_height), Image.Resampling.LANCZOS)

                    photo = ImageTk.PhotoImage(pil_image)

                    image_label = ttk.Label(right_frame, image=photo)
                    image_label.image = photo
                    image_label.pack(pady=10)

                    logger.info(f"Successfully loaded image for {card.get('card_name')}")
                else:
                    ttk.Label(right_frame, text="Failed to load image",
                              font=('Arial', 11)).pack(pady=50)
            except Exception as e:
                ttk.Label(right_frame, text="Image unavailable",
                          font=('Arial', 11)).pack(pady=50)
                logger.error(f"Error loading image: {e}")
        else:
            ttk.Label(right_frame, text="No image available",
                      font=('Arial', 11)).pack(pady=50)

    def _wrap_text(self, text: str, width: int = 70) -> str:
        import textwrap
        return textwrap.fill(text, width=width)


def main():
    logger.info("Starting Lucene Pokemon Card Search GUI")
    root = tk.Tk()
    app = LuceneSearchGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
